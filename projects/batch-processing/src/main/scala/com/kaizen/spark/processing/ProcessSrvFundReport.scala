package com.kaizen.spark.processing

import org.apache.spark.sql.{DataFrame, SparkSession}
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

    val fromTs =
      SparkIOUtils.formatIcebergDateBoundary(fromDate.getOrElse("1970-01-01"))
    val toTs = SparkIOUtils.formatIcebergDateBoundary(
      toDate.getOrElse(java.time.LocalDate.now().plusDays(1).toString)
    )

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.serving")

      println(
        s"Generating Serving Fund Reports (shop+PTTT / shop / order) for range: $fromTs to $toTs"
      )

      val factPayment = spark
        .table("nessie.dw.fact_payment_transaction")
        .filter(col("CreatedDate") >= fromTs && col("CreatedDate") < toTs)
      val orderFnd = spark.table("nessie.fnd.order")
      val returnFnd = spark.table("nessie.fnd.return")
      val dimShop = spark.table("nessie.dw.dim_shop").dropDuplicates("ShopCode")
      val paymentMethodFnd =
        spark.table("nessie.fnd.payment_method").dropDuplicates("Id")

      val validPayments = factPayment.filter(col("PaymentStatus") === "SUCCESS")

      // Resolve ReturnId for refund payments so cash + doc share the same grain key
      // Gắn ReturnId phiếu chi qua return_payment — tránh tách 2 dòng ± cùng số tiền
      val returnPaymentFnd = spark
        .table("nessie.fnd.return_payment")
        .select(
          col("PaymentId"),
          col("ReturnId").as("ReturnPaymentReturnId")
        )

      val enrichedPayments = validPayments
        .join(returnPaymentFnd, Seq("PaymentId"), "left")
        .withColumn(
          "GrainReturnId",
          when(
            col("PaymentType") === 2,
            coalesce(col("ReturnId"), col("ReturnPaymentReturnId"), lit(""))
          ).otherwise(lit(""))
        )

      // Option A: all fund metrics use payment CreatedDate (cash date) / Mọi metric quỹ theo ngày giao dịch payment
      val receiptPayments = enrichedPayments.filter(col("PaymentType") === 1)
      val expenditurePayments = enrichedPayments.filter(col("PaymentType") === 2)

      val receiptPaymentTotals = receiptPayments
        .groupBy("PaymentId")
        .agg(sum("Amount").as("PaymentTotalAmt"))

      val expenditurePaymentTotals = expenditurePayments
        .groupBy("PaymentId")
        .agg(sum("Amount").as("PaymentTotalAmt"))

      // Per-payment document total — avoids double-counting order.TotalPayment on multi-payment / Tổng phiếu theo PaymentId
      val paymentFndSuccess = spark
        .table("nessie.fnd.payment")
        .filter(col("Status") === "SUCCESS")
        .select(
          col("Id").as("PaymentHeaderId"),
          col("TotalPayment").cast("double").as("PaymentHeaderTotal")
        )

      // Detail grain: ReportDate + Shop + PaymentMethod / Chi tiết: ngày + shop + PTTT
      val cashFlowPaymentWithShop = enrichedPayments
        .join(orderFnd, enrichedPayments("OrderId") === orderFnd("Id"), "inner")
        .select(
          to_date(enrichedPayments("CreatedDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          enrichedPayments("PaymentMethodId").as("PaymentMethodId"),
          when(col("PaymentType") === 1, col("Amount"))
            .otherwise(lit(0.0))
            .as("ReceiptAmt"),
          when(col("PaymentType") === 2, col("Amount"))
            .otherwise(lit(0.0))
            .as("ExpenditureAmt")
        )

      val cashFlowAgg = cashFlowPaymentWithShop
        .groupBy("ReportDate", "ShopCode", "PaymentMethodId")
        .agg(
          sum("ReceiptAmt").as("TotalReceipts"),
          sum("ExpenditureAmt").as("TotalExpenditures")
        )

      // Order document side — allocated per payment header on cash date / Giá trị đơn theo phiếu thu, ngày payment
      val orderDocLines = receiptPayments
        .join(receiptPaymentTotals, Seq("PaymentId"), "inner")
        .join(
          paymentFndSuccess,
          receiptPayments("PaymentId") === paymentFndSuccess("PaymentHeaderId"),
          "inner"
        )
        .join(
          orderFnd.filter(col("Status").isin("Completed", "Returned")),
          receiptPayments("OrderId") === orderFnd("Id"),
          "inner"
        )
        .select(
          to_date(receiptPayments("CreatedDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          receiptPayments("PaymentMethodId").as("PaymentMethodId"),
          receiptPayments("OrderId").as("OrderId"),
          lit("").as("ReturnId"),
          (
            col("PaymentHeaderTotal") *
              (receiptPayments("Amount") / col("PaymentTotalAmt"))
          ).as("OrderAllocatedAmt")
        )

      val orderInvoiceAgg = orderDocLines
        .groupBy("ReportDate", "ShopCode", "PaymentMethodId")
        .agg(sum("OrderAllocatedAmt").as("TotalOrderAmount"))

      // Return document side — allocated per refund payment on cash date / Giá trị trả theo phiếu chi, ngày payment
      val returnDocLines = expenditurePayments
        .filter(col("GrainReturnId") =!= lit(""))
        .join(expenditurePaymentTotals, Seq("PaymentId"), "inner")
        .join(
          paymentFndSuccess,
          expenditurePayments("PaymentId") === paymentFndSuccess("PaymentHeaderId"),
          "inner"
        )
        .join(
          returnFnd,
          expenditurePayments("GrainReturnId") === returnFnd("Id"),
          "inner"
        )
        .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
        .select(
          to_date(expenditurePayments("CreatedDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          expenditurePayments("PaymentMethodId").as("PaymentMethodId"),
          returnFnd("OrderId").as("OrderId"),
          expenditurePayments("GrainReturnId").as("ReturnId"),
          (
            col("PaymentHeaderTotal") *
              (expenditurePayments("Amount") / col("PaymentTotalAmt"))
          ).as("ReturnAllocatedAmt")
        )

      val returnInvoiceAgg = returnDocLines
        .groupBy("ReportDate", "ShopCode", "PaymentMethodId")
        .agg(sum("ReturnAllocatedAmt").as("TotalReturnAmount"))

      val paymentKeys = cashFlowAgg
        .select("ReportDate", "ShopCode", "PaymentMethodId")
        .union(
          orderInvoiceAgg.select("ReportDate", "ShopCode", "PaymentMethodId")
        )
        .union(
          returnInvoiceAgg.select("ReportDate", "ShopCode", "PaymentMethodId")
        )
        .distinct()

      val paymentReportDf = withFundMetrics(
        paymentKeys
          .join(
            cashFlowAgg,
            Seq("ReportDate", "ShopCode", "PaymentMethodId"),
            "left"
          )
          .join(
            orderInvoiceAgg,
            Seq("ReportDate", "ShopCode", "PaymentMethodId"),
            "left"
          )
          .join(
            returnInvoiceAgg,
            Seq("ReportDate", "ShopCode", "PaymentMethodId"),
            "left"
          )
          .join(dimShop, Seq("ShopCode"), "left")
          .join(
            paymentMethodFnd,
            paymentKeys("PaymentMethodId") === paymentMethodFnd("Id"),
            "left"
          )
          .select(
            paymentKeys("ReportDate"),
            paymentKeys("ShopCode"),
            paymentKeys("PaymentMethodId").as("PaymentMethodId"),
            coalesce(dimShop("ShopName"), lit("Unknown")).as("ShopName"),
            coalesce(paymentMethodFnd("Name"), lit("Unknown"))
              .as("PaymentMethodName"),
            coalesce(col("TotalReceipts"), lit(0.0)).as("TotalReceipts"),
            coalesce(col("TotalExpenditures"), lit(0.0)).as("TotalExpenditures"),
            coalesce(col("TotalOrderAmount"), lit(0.0)).as("TotalOrderAmount"),
            coalesce(col("TotalReturnAmount"), lit(0.0)).as("TotalReturnAmount")
          )
      )

      // Shop aggregate — rollup from payment detail / Gom theo cửa hàng
      val shopReportDf = withFundMetrics(
        paymentReportDf
          .groupBy(col("ReportDate"), col("ShopCode"))
          .agg(
            first(col("ShopName"), ignoreNulls = true).as("ShopName"),
            sum(col("TotalReceipts")).as("TotalReceipts"),
            sum(col("TotalExpenditures")).as("TotalExpenditures"),
            sum(col("TotalOrderAmount")).as("TotalOrderAmount"),
            sum(col("TotalReturnAmount")).as("TotalReturnAmount")
          )
      )

      // Order grain — sale rows (ReturnId="") + return rows built in one pass per payment
      // Grain đơn: bán (ReturnId="") + hoàn (expenditure + doc cùng dòng payment)
      val saleCashByOrder = enrichedPayments
        .filter(col("PaymentType") === 1)
        .join(orderFnd, enrichedPayments("OrderId") === orderFnd("Id"), "inner")
        .select(
          orderFnd("Id").as("OrderId"),
          lit("").as("ReturnId"),
          orderFnd("ShopCode").as("ShopCode"),
          enrichedPayments("PaymentMethodId").as("PaymentMethodId"),
          to_date(enrichedPayments("CreatedDate")).as("ReportDate"),
          enrichedPayments("Amount").as("ReceiptAmt")
        )
        .groupBy(
          "ReportDate",
          "OrderId",
          "ReturnId",
          "ShopCode",
          "PaymentMethodId"
        )
        .agg(sum("ReceiptAmt").as("TotalReceipts"))

      val orderInvoiceByOrder = orderDocLines
        .groupBy(
          "ReportDate",
          "OrderId",
          "ReturnId",
          "ShopCode",
          "PaymentMethodId"
        )
        .agg(sum("OrderAllocatedAmt").as("TotalOrderAmount"))

      // Return: cash + document from the SAME payment line / Hoàn: thu+chi+doc trên cùng 1 payment
      val returnOrderBase = expenditurePayments
        .filter(col("GrainReturnId") =!= lit(""))
        .join(expenditurePaymentTotals, Seq("PaymentId"), "inner")
        .join(
          paymentFndSuccess,
          expenditurePayments("PaymentId") === paymentFndSuccess("PaymentHeaderId"),
          "inner"
        )
        .join(
          returnFnd,
          expenditurePayments("GrainReturnId") === returnFnd("Id"),
          "inner"
        )
        .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
        .select(
          to_date(expenditurePayments("CreatedDate")).as("ReportDate"),
          returnFnd("OrderId").as("OrderId"),
          expenditurePayments("GrainReturnId").as("ReturnId"),
          orderFnd("ShopCode").as("ShopCode"),
          expenditurePayments("PaymentMethodId").as("PaymentMethodId"),
          expenditurePayments("Amount").cast("double").as("ExpenditureAmt"),
          (
            col("PaymentHeaderTotal") *
              (expenditurePayments("Amount") / col("PaymentTotalAmt"))
          ).as("ReturnAllocatedAmt")
        )

      val returnOrderMetrics = returnOrderBase
        .groupBy("ReportDate", "OrderId", "ReturnId", "ShopCode", "PaymentMethodId")
        .agg(
          sum("ExpenditureAmt").as("TotalExpenditures"),
          sum("ReturnAllocatedAmt").as("TotalReturnAmount")
        )

      val saleOrderKeys = saleCashByOrder
        .select(
          "ReportDate",
          "OrderId",
          "ReturnId",
          "ShopCode",
          "PaymentMethodId"
        )
        .union(
          orderInvoiceByOrder.select(
            "ReportDate",
            "OrderId",
            "ReturnId",
            "ShopCode",
            "PaymentMethodId"
          )
        )
        .distinct()

      val saleOrderPart = saleOrderKeys
        .join(
          saleCashByOrder,
          Seq("ReportDate", "OrderId", "ReturnId", "ShopCode", "PaymentMethodId"),
          "left"
        )
        .join(
          orderInvoiceByOrder,
          Seq("ReportDate", "OrderId", "ReturnId", "ShopCode", "PaymentMethodId"),
          "left"
        )
        .join(dimShop, Seq("ShopCode"), "left")
        .join(
          paymentMethodFnd,
          saleOrderKeys("PaymentMethodId") === paymentMethodFnd("Id"),
          "left"
        )
        .select(
          saleOrderKeys("ReportDate"),
          saleOrderKeys("OrderId"),
          saleOrderKeys("ReturnId"),
          saleOrderKeys("ShopCode"),
          saleOrderKeys("PaymentMethodId").as("PaymentMethodId"),
          coalesce(dimShop("ShopName"), lit("Unknown")).as("ShopName"),
          coalesce(paymentMethodFnd("Name"), lit("Unknown")).as("PaymentMethodName"),
          coalesce(col("TotalReceipts"), lit(0.0)).as("TotalReceipts"),
          lit(0.0).as("TotalExpenditures"),
          coalesce(col("TotalOrderAmount"), lit(0.0)).as("TotalOrderAmount"),
          lit(0.0).as("TotalReturnAmount")
        )

      val returnOrderPart = returnOrderMetrics
        .join(dimShop, Seq("ShopCode"), "left")
        .join(
          paymentMethodFnd,
          returnOrderMetrics("PaymentMethodId") === paymentMethodFnd("Id"),
          "left"
        )
        .select(
          col("ReportDate"),
          col("OrderId"),
          col("ReturnId"),
          col("ShopCode"),
          col("PaymentMethodId"),
          coalesce(dimShop("ShopName"), lit("Unknown")).as("ShopName"),
          coalesce(paymentMethodFnd("Name"), lit("Unknown")).as("PaymentMethodName"),
          lit(0.0).as("TotalReceipts"),
          coalesce(col("TotalExpenditures"), lit(0.0)).as("TotalExpenditures"),
          lit(0.0).as("TotalOrderAmount"),
          coalesce(col("TotalReturnAmount"), lit(0.0)).as("TotalReturnAmount")
        )

      val orderReportDf = withFundMetrics(
        saleOrderPart.unionByName(returnOrderPart)
      )

      writeServingTable(
        spark,
        paymentReportDf,
        "nessie.serving.fund_report",
        "nessie.stg.fund_report",
        Seq("ReportDate", "ShopCode", "PaymentMethodId")
      )
      writeServingTable(
        spark,
        shopReportDf,
        "nessie.serving.fund_report_shop",
        "nessie.stg.fund_report_shop",
        Seq("ReportDate", "ShopCode")
      )
      writeServingTable(
        spark,
        orderReportDf,
        "nessie.serving.fund_report_order",
        "nessie.stg.fund_report_order",
        Seq("ReportDate", "OrderId", "ReturnId", "PaymentMethodId")
      )

      println("✔ Serving Fund Reports (shop+PTTT / shop / order) completed.")
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Seq(
          "nessie.stg.fund_report",
          "nessie.stg.fund_report_shop",
          "nessie.stg.fund_report_order",
          "nessie.serving.fund_report",
          "nessie.serving.fund_report_shop",
          "nessie.serving.fund_report_order"
        ).foreach(t => Try(SparkIOUtils.expireTableSnapshots(spark, t)))
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed with error:")
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }

  private def withFundMetrics(df: DataFrame): DataFrame = {
    df
      .withColumn(
        "NetCashFlow",
        coalesce(col("TotalReceipts"), lit(0.0)) - coalesce(
          col("TotalExpenditures"),
          lit(0.0)
        )
      )
      .withColumn(
        "ReconciliationDiff",
        col("NetCashFlow") -
          (coalesce(col("TotalOrderAmount"), lit(0.0)) - coalesce(
            col("TotalReturnAmount"),
            lit(0.0)
          ))
      )
  }

  private def writeServingTable(
      spark: SparkSession,
      reportDf: DataFrame,
      targetTable: String,
      stgTable: String,
      mergeKeys: Seq[String]
  ): Unit = {
    val targetExists = SparkIOUtils.tableExists(spark, targetTable)
    if (!targetExists) {
      println(s"Initializing target table '$targetTable'...")
      SparkIOUtils.writeToIceberg(spark, reportDf, stgTable, "overwrite")
      val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
      SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
    } else {
      println(s"Merging into '$targetTable'...")
      SparkIOUtils.writeToIceberg(spark, reportDf, stgTable, "overwrite")
      val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
      SparkIOUtils.mergeIcebergTable(spark, stgDf, targetTable, mergeKeys)
    }
  }

}
