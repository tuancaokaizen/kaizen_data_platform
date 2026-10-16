package com.kaizen.batch.publish.clickhouse

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Builds SALE + RETURN grains for realtime_business_by_order from lakehouse fnd (same pairing as Flink).
  * Dựng grain SALE + RETURN cho realtime_business_by_order từ fnd (cùng cặp như Flink).
  */
object RealtimeBusinessByOrder {

  val ServingTable: String = "nessie.serving.realtime_business_by_order"
  val StagingTable: String = "nessie.stg.realtime_business_by_order"
  val MergeKeys: Seq[String] = Seq("OrderId", "ReturnId")
  val HotTable: String = "report.realtime_business_by_order"

  /** Rows for [fromDate, toDate) on TransactionDate / CreatedDate. / Dòng cho [fromDate, toDate) theo TransactionDate / CreatedDate. */
  def fromLakehouse(spark: SparkSession, fromDate: String, toDate: String): DataFrame = {
    require(SparkUtils.tableExists(spark, "nessie.fnd.order"), "Required table missing: nessie.fnd.order")
    require(SparkUtils.tableExists(spark, "nessie.fnd.payment"), "Required table missing: nessie.fnd.payment")
    require(SparkUtils.tableExists(spark, "nessie.fnd.order_payment"), "Required table missing: nessie.fnd.order_payment")

    val fromTs = s"$fromDate 00:00:00"
    val toTs = s"$toDate 00:00:00"

    val orderFnd = spark.table("nessie.fnd.order")
      .where(col("TransactionDate") >= fromTs && col("TransactionDate") < toTs && col("Status").isin("Completed", "Returned"))

    val dimShop =
      if (SparkUtils.tableExists(spark, "nessie.dw.dim_shop"))
        spark.table("nessie.dw.dim_shop").dropDuplicates("ShopCode")
      else
        orderFnd.select(col("ShopCode")).dropDuplicates().withColumn("ShopName", col("ShopCode"))

    val paymentMethodFnd =
      if (SparkUtils.tableExists(spark, "nessie.fnd.payment_method"))
        Some(spark.table("nessie.fnd.payment_method").dropDuplicates("Id"))
      else None

    val paymentFnd = spark.table("nessie.fnd.payment").where(col("Status") === "SUCCESS")
    val orderPayment = spark.table("nessie.fnd.order_payment")

    val txFnd =
      if (SparkUtils.tableExists(spark, "nessie.fnd.transaction"))
        Some(spark.table("nessie.fnd.transaction").select(col("PaymentId"), col("PaymentMethodId"), col("Amount")))
      else None

    val returnPayment =
      if (SparkUtils.tableExists(spark, "nessie.fnd.return_payment")) Some(spark.table("nessie.fnd.return_payment")) else None

    val returnFnd =
      if (SparkUtils.tableExists(spark, "nessie.fnd.return"))
        Some(spark.table("nessie.fnd.return").where(col("CreatedDate") >= fromTs && col("CreatedDate") < toTs))
      else None

    val saleOrders = orderFnd
      .join(dimShop, Seq("ShopCode"), "left")
      .select(
        to_date(col("TransactionDate")).as("ReportDate"),
        col("Id").as("OrderId"),
        lit("").as("ReturnId"),
        lit("SALE").as("DocumentType"),
        col("ShopCode"),
        coalesce(col("ShopName"), col("ShopCode"), lit("Unknown")).as("ShopName"),
        col("Status").as("OrderStatus"),
        col("TotalPayment").cast("long").as("SalesAmount"),
        lit(0L).as("ReturnAmount"),
        col("TotalPayment").cast("long").as("NetRevenue"),
        col("TotalPayment").cast("long").as("TotalOrderAmount"),
        lit(0L).as("TotalReturnAmount")
      )

    val salePayments = paymentFnd
      .where(col("PaymentType") === 1)
      .join(orderPayment, paymentFnd("Id") === orderPayment("PaymentId"), "inner")
      .select(
        orderPayment("OrderId").as("OrderId"),
        paymentFnd("Id").as("PaymentId"),
        paymentFnd("TotalPayment").cast("long").as("PayAmt")
      )

    val saleReceipts = salePayments.groupBy("OrderId").agg(sum("PayAmt").as("TotalReceipts"))
    val saleMethodSummary = methodSummary(salePayments, txFnd, paymentMethodFnd, withReturnId = false)

    // INNER join — only orders with a SUCCESS receipt / Chỉ đơn đã có phiếu thu SUCCESS
    val saleDf = saleOrders
      .join(saleReceipts, Seq("OrderId"), "inner")
      .join(saleMethodSummary, Seq("OrderId"), "left")
      .withColumn("TotalReceipts", coalesce(col("TotalReceipts"), lit(0L)))
      .withColumn("TotalExpenditures", lit(0L))
      .withColumn("PaymentMethodSummary", coalesce(col("PaymentMethodSummary"), lit("")))
      .where(col("TotalReceipts") > lit(0L))

    val returnDf = (returnFnd, returnPayment) match {
      case (Some(returns), Some(returnPay)) =>
        val returnOrders = returns
          .join(
            orderFnd.select(col("Id").as("OrdId"), col("ShopCode"), col("Status")),
            returns("OrderId") === col("OrdId"),
            "inner"
          )
          .join(dimShop, Seq("ShopCode"), "left")
          .select(
            to_date(returns("CreatedDate")).as("ReportDate"),
            returns("OrderId").as("OrderId"),
            returns("Id").as("ReturnId"),
            lit("RETURN").as("DocumentType"),
            col("ShopCode"),
            coalesce(col("ShopName"), col("ShopCode"), lit("Unknown")).as("ShopName"),
            lit("Returned").as("OrderStatus"),
            lit(0L).as("SalesAmount"),
            coalesce(returns("ReturnAmount"), lit(0)).cast("long").as("ReturnAmount"),
            (lit(0L) - coalesce(returns("ReturnAmount"), lit(0)).cast("long")).as("NetRevenue"),
            lit(0L).as("TotalOrderAmount"),
            coalesce(returns("ReturnAmount"), lit(0)).cast("long").as("TotalReturnAmount")
          )

        val returnPayments = paymentFnd
          .where(col("PaymentType") === 2)
          .join(returnPay, paymentFnd("Id") === returnPay("PaymentId"), "inner")
          .select(
            returnPay("OrderId").as("OrderId"),
            returnPay("ReturnId").as("ReturnId"),
            paymentFnd("Id").as("PaymentId"),
            paymentFnd("TotalPayment").cast("long").as("PayAmt")
          )

        val returnSpend = returnPayments.groupBy("OrderId", "ReturnId").agg(max("PayAmt").as("TotalExpenditures"))
        val returnMethodSummary = methodSummary(returnPayments, txFnd, paymentMethodFnd, withReturnId = true)

        returnOrders
          .join(returnSpend, Seq("OrderId", "ReturnId"), "inner")
          .join(returnMethodSummary, Seq("OrderId", "ReturnId"), "left")
          .withColumn("TotalReceipts", lit(0L))
          .withColumn("TotalExpenditures", coalesce(col("TotalExpenditures"), lit(0L)))
          .withColumn("PaymentMethodSummary", coalesce(col("PaymentMethodSummary"), lit("")))
      case _ =>
        saleDf.limit(0)
    }

    saleDf
      .unionByName(returnDf)
      .withColumn("NetCashFlow", col("TotalReceipts") - col("TotalExpenditures"))
      .withColumn("ReconciliationDiff", col("NetCashFlow") - (col("TotalOrderAmount") - col("TotalReturnAmount")))
      .withColumn("PendingOrder", lit(0).cast(ByteType))
      .withColumn("Version", lit(1L))
      .withColumn("UpdatedAt", current_timestamp())
      .select(
        col("ReportDate"), col("OrderId"), col("ReturnId"), col("DocumentType"), col("ShopCode"), col("ShopName"),
        col("OrderStatus"), col("SalesAmount"), col("ReturnAmount"), col("NetRevenue"), col("TotalReceipts"),
        col("TotalExpenditures"), col("NetCashFlow"), col("PaymentMethodSummary"), col("TotalOrderAmount"),
        col("TotalReturnAmount"), col("ReconciliationDiff"), col("PendingOrder"), col("Version"), col("UpdatedAt")
      )
      .withColumn(
        "rn",
        row_number().over(Window.partitionBy("OrderId", "ReturnId").orderBy(col("ReportDate").desc, col("UpdatedAt").desc))
      )
      .where(col("rn") === 1)
      .drop("rn")
  }

  private def methodSummary(
      payments: DataFrame,
      txFnd: Option[DataFrame],
      paymentMethodFnd: Option[DataFrame],
      withReturnId: Boolean
  ): DataFrame = {
    val keys = if (withReturnId) Seq("OrderId", "ReturnId") else Seq("OrderId")
    txFnd match {
      case None =>
        payments.select(keys.map(col): _*).dropDuplicates().withColumn("PaymentMethodSummary", lit(""))
      case Some(tx) =>
        val methodName = paymentMethodFnd.map { methods =>
          methods.select(col("Id").as("PaymentMethodId"), col("Name").as("MethodName"))
        }
        val payKeys =
          if (withReturnId) payments.select("OrderId", "ReturnId", "PaymentId", "PayAmt")
          else payments.select("OrderId", "PaymentId", "PayAmt")

        val withMethodBase = payKeys
          .join(tx, payKeys("PaymentId") === tx("PaymentId"), "left")
          .withColumn("LineAmt", coalesce(col("Amount").cast("long"), col("PayAmt")))

        val withMethod = methodName match {
          case Some(names) =>
            withMethodBase
              .join(names, Seq("PaymentMethodId"), "left")
              .withColumn("MethodLabel", coalesce(col("MethodName"), concat(lit("PTTT"), col("PaymentMethodId"))))
          case None =>
            withMethodBase.withColumn("MethodLabel", concat(lit("PTTT"), col("PaymentMethodId")))
        }

        val groupCols =
          if (withReturnId) Seq(col("OrderId"), col("ReturnId"), col("MethodLabel"))
          else Seq(col("OrderId"), col("MethodLabel"))

        val perMethod = withMethod
          .groupBy(groupCols: _*)
          .agg(sum("LineAmt").as("MethodAmt"))
          .withColumn("Part", concat(col("MethodLabel"), lit(":"), col("MethodAmt").cast("long")))

        if (withReturnId)
          perMethod.groupBy("OrderId", "ReturnId").agg(concat_ws("|", collect_list(col("Part"))).as("PaymentMethodSummary"))
        else
          perMethod.groupBy("OrderId").agg(concat_ws("|", collect_list(col("Part"))).as("PaymentMethodSummary"))
    }
  }
}
