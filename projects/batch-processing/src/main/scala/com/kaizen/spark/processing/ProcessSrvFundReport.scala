package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessSrvFundReport {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDate = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
    val toDate = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

    val jobName = "ProcessSrvFundReport"
    println(s"=== Starting $jobName ===")
    fromDate.foreach(d => println(s"From Date: $d"))
    toDate.foreach(d => println(s"To Date: $d"))

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val fromTs = formatTimestamp(fromDate.getOrElse("1970-01-01"), isEndOfDay = false)
    val toTs = formatTimestamp(toDate.getOrElse(java.time.LocalDate.now().plusDays(1).toString), isEndOfDay = false)

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.serving")

      val targetTable = "nessie.serving.fund_report"
      val stgTable = "nessie.stg.fund_report"

      println(s"Generating Serving Fund Report for range: $fromTs to $toTs")

      // Load DW and Foundation tables
      val factPayment = spark.table("nessie.dw.fact_payment_transaction")
        .filter(col("CreatedDate") >= fromTs && col("CreatedDate") < toTs)
      val orderFnd = spark.table("nessie.fnd.order")
      val returnFnd = spark.table("nessie.fnd.return")
      val dimShop = spark.table("nessie.dw.dim_shop")
      val paymentMethodFnd = spark.table("nessie.fnd.payment_method")

      // 1. Giao dịch thực tế (Dòng tiền - Cash Flow)
      // Chỉ lấy các giao dịch thanh toán thành công (PaymentStatus = 'SUCCESS')
      val validPayments = factPayment
        .filter(col("PaymentStatus") === "SUCCESS")

      // Aggregation 1: Dòng tiền thực tế (Cash Flow) theo ngày, cửa hàng, phương thức thanh toán
      val cashFlowPaymentWithShop = validPayments
        .join(orderFnd, validPayments("OrderId") === orderFnd("Id"), "inner")
        .select(
          to_date(validPayments("CreatedDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          validPayments("PaymentMethodId").as("PaymentMethodId"),
          when(col("PaymentType") === 1, col("Amount")).otherwise(lit(0.0)).as("ReceiptAmt"),
          when(col("PaymentType") === 2, col("Amount")).otherwise(lit(0.0)).as("ExpenditureAmt")
        )

      val cashFlowAgg = cashFlowPaymentWithShop
        .groupBy("ReportDate", "ShopCode", "PaymentMethodId")
        .agg(
          sum("ReceiptAmt").as("TotalReceipts"),
          sum("ExpenditureAmt").as("TotalExpenditures")
        )

      // 2. Doanh thu và hoàn trả hạch toán hóa đơn (Invoice / Order / Return)
      // Phân bổ TotalPayment của Order theo tỷ lệ đóng góp của từng phương thức thanh toán trong transaction
      val ptTotalReceipt = validPayments
        .filter(col("PaymentType") === 1)
        .groupBy("PaymentId")
        .agg(sum("Amount").as("PaymentTotalAmt"))

      val orderPaymentSplitRatio = validPayments
        .filter(col("PaymentType") === 1)
        .join(ptTotalReceipt, Seq("PaymentId"), "inner")
        .select(
          col("OrderId"),
          col("PaymentMethodId"),
          (col("Amount") / col("PaymentTotalAmt")).as("SplitRatio")
        )

      val orderInvoiceDetails = orderFnd
        .filter(col("Status").isin("Completed", "Returned"))
        .join(orderPaymentSplitRatio, orderFnd("Id") === orderPaymentSplitRatio("OrderId"), "inner")
        .select(
          to_date(orderFnd("TransactionDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          orderPaymentSplitRatio("PaymentMethodId").as("PaymentMethodId"),
          (orderFnd("TotalPayment") * orderPaymentSplitRatio("SplitRatio")).as("OrderAllocatedAmt")
        )

      val orderInvoiceAgg = orderInvoiceDetails
        .groupBy("ReportDate", "ShopCode", "PaymentMethodId")
        .agg(sum("OrderAllocatedAmt").as("TotalOrderAmount"))

      // Tiền hoàn trả hạch toán (Return) phân bổ theo phương thức thanh toán
      val ptTotalExpenditure = validPayments
        .filter(col("PaymentType") === 2)
        .groupBy("PaymentId")
        .agg(sum("Amount").as("PaymentTotalAmt"))

      val returnPaymentSplitRatio = validPayments
        .filter(col("PaymentType") === 2)
        .join(ptTotalExpenditure, Seq("PaymentId"), "inner")
        .select(
          col("OrderId"),
          col("ReturnId"),
          col("PaymentMethodId"),
          (col("Amount") / col("PaymentTotalAmt")).as("SplitRatio")
        )

      val returnInvoiceDetails = returnFnd
        .join(returnPaymentSplitRatio, returnFnd("Id") === returnPaymentSplitRatio("ReturnId"), "inner")
        .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
        .select(
          to_date(returnFnd("CreatedDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          returnPaymentSplitRatio("PaymentMethodId").as("PaymentMethodId"),
          (returnFnd("ReturnAmount") * returnPaymentSplitRatio("SplitRatio")).as("ReturnAllocatedAmt")
        )

      val returnInvoiceAgg = returnInvoiceDetails
        .groupBy("ReportDate", "ShopCode", "PaymentMethodId")
        .agg(sum("ReturnAllocatedAmt").as("TotalReturnAmount"))

      // 3. Union distinct keys (ReportDate, ShopCode, PaymentMethodId) từ cả 3 nguồn
      val allKeys = cashFlowAgg.select("ReportDate", "ShopCode", "PaymentMethodId")
        .union(orderInvoiceAgg.select("ReportDate", "ShopCode", "PaymentMethodId"))
        .union(returnInvoiceAgg.select("ReportDate", "ShopCode", "PaymentMethodId"))
        .distinct()

      // Ghép dữ liệu và bổ sung thông tin hiển thị
      val reportDf = allKeys
        .join(cashFlowAgg, Seq("ReportDate", "ShopCode", "PaymentMethodId"), "left")
        .join(orderInvoiceAgg, Seq("ReportDate", "ShopCode", "PaymentMethodId"), "left")
        .join(returnInvoiceAgg, Seq("ReportDate", "ShopCode", "PaymentMethodId"), "left")
        .join(dimShop, Seq("ShopCode"), "left")
        .join(paymentMethodFnd, allKeys("PaymentMethodId") === paymentMethodFnd("Id"), "left")
        .select(
          allKeys("ReportDate"),
          allKeys("ShopCode"),
          coalesce(dimShop("ShopName"), lit("Unknown")).as("ShopName"),
          coalesce(paymentMethodFnd("Name"), lit("Unknown")).as("PaymentMethodName"),
          coalesce(col("TotalReceipts"), lit(0.0)).as("TotalReceipts"),
          coalesce(col("TotalExpenditures"), lit(0.0)).as("TotalExpenditures"),
          (coalesce(col("TotalReceipts"), lit(0.0)) - coalesce(col("TotalExpenditures"), lit(0.0))).as("NetCashFlow"),
          coalesce(col("TotalOrderAmount"), lit(0.0)).as("TotalOrderAmount"),
          coalesce(col("TotalReturnAmount"), lit(0.0)).as("TotalReturnAmount"),
          ((coalesce(col("TotalReceipts"), lit(0.0)) - coalesce(col("TotalExpenditures"), lit(0.0))) - 
            (coalesce(col("TotalOrderAmount"), lit(0.0)) - coalesce(col("TotalReturnAmount"), lit(0.0)))).as("ReconciliationDiff")
        )

      // Write to Iceberg Serving
      val targetExists = SparkIOUtils.tableExists(spark, targetTable)
      if (!targetExists) {
        println(s"Target table '$targetTable' does not exist. Initializing table...")
        SparkIOUtils.writeToIceberg(spark, reportDf, stgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
        SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
      } else {
        println(s"Merging report records into target table '$targetTable'...")
        SparkIOUtils.writeToIceberg(spark, reportDf, stgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
        SparkIOUtils.mergeIcebergTable(spark, stgDf, targetTable, Seq("ReportDate", "ShopCode", "PaymentMethodName"))
      }
      println(s"✔ Serving Fund Report generation completed for: $targetTable")
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(spark, "nessie.stg.fund_report")
          SparkIOUtils.expireTableSnapshots(spark, "nessie.serving.fund_report")
        }
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed with error:")
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }

  private def formatTimestamp(dateStr: String, isEndOfDay: Boolean): String = {
    val trimmed = dateStr.trim
    if (trimmed.length == 10) {
      if (isEndOfDay) s"$trimmed 23:59:59" else s"$trimmed 00:00:00"
    } else {
      trimmed
    }
  }
}
