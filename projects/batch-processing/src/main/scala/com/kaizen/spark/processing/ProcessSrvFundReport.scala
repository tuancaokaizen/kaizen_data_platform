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
      formatTimestamp(fromDate.getOrElse("1970-01-01"), isEndOfDay = false)
    val toTs = formatTimestamp(
      toDate.getOrElse(java.time.LocalDate.now().plusDays(1).toString),
      isEndOfDay = false
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
      val dimShop = spark.table("nessie.dw.dim_shop")
      val paymentMethodFnd = spark.table("nessie.fnd.payment_method")

      val validPayments = factPayment.filter(col("PaymentStatus") === "SUCCESS")

      // Detail grain: ReportDate + Shop + PaymentMethod / Chi tiết: ngày + shop + PTTT
      val cashFlowPaymentWithShop = validPayments
        .join(orderFnd, validPayments("OrderId") === orderFnd("Id"), "inner")
        .select(
          to_date(validPayments("CreatedDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          validPayments("PaymentMethodId").as("PaymentMethodId"),
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
        .join(
          orderPaymentSplitRatio,
          orderFnd("Id") === orderPaymentSplitRatio("OrderId"),
          "inner"
        )
        .select(
          to_date(orderFnd("TransactionDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          orderPaymentSplitRatio("PaymentMethodId").as("PaymentMethodId"),
          (orderFnd("TotalPayment") * orderPaymentSplitRatio("SplitRatio"))
            .as("OrderAllocatedAmt")
        )

      val orderInvoiceAgg = orderInvoiceDetails
        .groupBy("ReportDate", "ShopCode", "PaymentMethodId")
        .agg(sum("OrderAllocatedAmt").as("TotalOrderAmount"))

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
        .join(
          returnPaymentSplitRatio,
          returnFnd("Id") === returnPaymentSplitRatio("ReturnId"),
          "inner"
        )
        .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
        .select(
          to_date(returnFnd("CreatedDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          returnPaymentSplitRatio("PaymentMethodId").as("PaymentMethodId"),
          (returnFnd("ReturnAmount") * returnPaymentSplitRatio("SplitRatio"))
            .as("ReturnAllocatedAmt")
        )

      val returnInvoiceAgg = returnInvoiceDetails
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
          .groupBy(col("ReportDate"), col("ShopCode"), col("ShopName"))
          .agg(
            sum(col("TotalReceipts")).as("TotalReceipts"),
            sum(col("TotalExpenditures")).as("TotalExpenditures"),
            sum(col("TotalOrderAmount")).as("TotalOrderAmount"),
            sum(col("TotalReturnAmount")).as("TotalReturnAmount")
          )
      )

      // Order grain: OrderId + PaymentMethod (+ ReturnId for refunds) / Grain theo đơn
      val cashFlowByOrder = validPayments
        .join(orderFnd, validPayments("OrderId") === orderFnd("Id"), "inner")
        .select(
          orderFnd("Id").as("OrderId"),
          coalesce(validPayments("ReturnId"), lit("")).as("ReturnId"),
          orderFnd("ShopCode").as("ShopCode"),
          validPayments("PaymentMethodId").as("PaymentMethodId"),
          to_date(
            coalesce(
              orderFnd("TransactionDate"),
              validPayments("CreatedDate")
            )
          ).as("ReportDate"),
          when(col("PaymentType") === 1, col("Amount"))
            .otherwise(lit(0.0))
            .as("ReceiptAmt"),
          when(col("PaymentType") === 2, col("Amount"))
            .otherwise(lit(0.0))
            .as("ExpenditureAmt")
        )
        .groupBy(
          "ReportDate",
          "OrderId",
          "ReturnId",
          "ShopCode",
          "PaymentMethodId"
        )
        .agg(
          sum("ReceiptAmt").as("TotalReceipts"),
          sum("ExpenditureAmt").as("TotalExpenditures")
        )

      val orderLevelInvoiceDetails = orderFnd
        .filter(col("Status").isin("Completed", "Returned"))
        .join(
          orderPaymentSplitRatio,
          orderFnd("Id") === orderPaymentSplitRatio("OrderId"),
          "inner"
        )
        .select(
          orderFnd("Id").as("OrderId"),
          lit("").as("ReturnId"),
          orderFnd("ShopCode").as("ShopCode"),
          orderPaymentSplitRatio("PaymentMethodId").as("PaymentMethodId"),
          to_date(orderFnd("TransactionDate")).as("ReportDate"),
          (orderFnd("TotalPayment") * orderPaymentSplitRatio("SplitRatio"))
            .as("OrderAllocatedAmt")
        )

      val orderInvoiceByOrder = orderLevelInvoiceDetails
        .groupBy(
          "ReportDate",
          "OrderId",
          "ReturnId",
          "ShopCode",
          "PaymentMethodId"
        )
        .agg(sum("OrderAllocatedAmt").as("TotalOrderAmount"))

      val returnInvoiceByOrder = returnFnd
        .join(
          returnPaymentSplitRatio,
          returnFnd("Id") === returnPaymentSplitRatio("ReturnId"),
          "inner"
        )
        .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
        .select(
          returnFnd("OrderId").as("OrderId"),
          returnFnd("Id").as("ReturnId"),
          orderFnd("ShopCode").as("ShopCode"),
          returnPaymentSplitRatio("PaymentMethodId").as("PaymentMethodId"),
          to_date(returnFnd("CreatedDate")).as("ReportDate"),
          (returnFnd("ReturnAmount") * returnPaymentSplitRatio("SplitRatio"))
            .as("ReturnAllocatedAmt")
        )
        .groupBy(
          "ReportDate",
          "OrderId",
          "ReturnId",
          "ShopCode",
          "PaymentMethodId"
        )
        .agg(sum("ReturnAllocatedAmt").as("TotalReturnAmount"))

      val orderKeys = cashFlowByOrder
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
        .union(
          returnInvoiceByOrder.select(
            "ReportDate",
            "OrderId",
            "ReturnId",
            "ShopCode",
            "PaymentMethodId"
          )
        )
        .distinct()

      val orderReportDf = withFundMetrics(
        orderKeys
          .join(
            cashFlowByOrder,
            Seq(
              "ReportDate",
              "OrderId",
              "ReturnId",
              "ShopCode",
              "PaymentMethodId"
            ),
            "left"
          )
          .join(
            orderInvoiceByOrder,
            Seq(
              "ReportDate",
              "OrderId",
              "ReturnId",
              "ShopCode",
              "PaymentMethodId"
            ),
            "left"
          )
          .join(
            returnInvoiceByOrder,
            Seq(
              "ReportDate",
              "OrderId",
              "ReturnId",
              "ShopCode",
              "PaymentMethodId"
            ),
            "left"
          )
          .join(dimShop, Seq("ShopCode"), "left")
          .join(
            paymentMethodFnd,
            orderKeys("PaymentMethodId") === paymentMethodFnd("Id"),
            "left"
          )
          .select(
            orderKeys("ReportDate"),
            orderKeys("OrderId"),
            orderKeys("ReturnId"),
            orderKeys("ShopCode"),
            coalesce(dimShop("ShopName"), lit("Unknown")).as("ShopName"),
            coalesce(paymentMethodFnd("Name"), lit("Unknown"))
              .as("PaymentMethodName"),
            coalesce(col("TotalReceipts"), lit(0.0)).as("TotalReceipts"),
            coalesce(col("TotalExpenditures"), lit(0.0)).as("TotalExpenditures"),
            coalesce(col("TotalOrderAmount"), lit(0.0)).as("TotalOrderAmount"),
            coalesce(col("TotalReturnAmount"), lit(0.0)).as("TotalReturnAmount")
          )
      )

      writeServingTable(
        spark,
        paymentReportDf,
        "nessie.serving.fund_report",
        "nessie.stg.fund_report",
        Seq("ReportDate", "ShopCode", "PaymentMethodName")
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
        Seq("OrderId", "ReturnId", "PaymentMethodName")
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

  private def formatTimestamp(dateStr: String, isEndOfDay: Boolean): String = {
    val trimmed = dateStr.trim
    if (trimmed.length == 10) {
      if (isEndOfDay) s"$trimmed 23:59:59" else s"$trimmed 00:00:00"
    } else {
      trimmed
    }
  }
}
