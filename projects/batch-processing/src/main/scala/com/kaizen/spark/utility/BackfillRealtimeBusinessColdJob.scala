package com.kaizen.spark.utility

import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ByteType
import scala.util.{Failure, Success, Try}

/** One-time backfill of biz_rt_order from lakehouse (fnd) into cold (+ optional hot seed).
  * Backfill một lần từ lakehouse vào cold (và seed hot nếu thiếu cửa sổ 14 ngày).
  *
  * Why: Kafka retention ~2d — stream cannot cover 2026-01-01 → stream start.
  * MigrateBizRtToColdJob only moves rows already in report.biz_rt_order hot.
  *
  * Args:
  *   --fromDate 2026-01-01
  *   --toDate   2026-07-04   (exclusive end, default = tomorrow)
  *   --hotDays  14
  *   --seedHot  true|false   (also INSERT recent window into report.biz_rt_order)
  */
object BackfillBizRtColdJob {

  private val ServingTable = "nessie.serving.biz_rt_order"
  private val StgTable = "nessie.stg.biz_rt_order"
  private val MergeKeys = Seq("OrderId", "ReturnId")

  def main(args: Array[String]): Unit = {
    val jobName = "BackfillBizRtColdJob"
    val cli = SparkIOUtils.parseArgs(args)

    val today =
      java.time.LocalDate.now(java.time.ZoneId.of("Asia/Ho_Chi_Minh"))
    val fromDate = cli.getOrElse("fromDate", "2026-01-01")
    val toDate = cli.getOrElse("toDate", today.plusDays(1).toString)
    val hotDays = cli.get("hotDays").map(_.toInt).getOrElse(14)
    val seedHot = cli.get("seedHot").forall(!_.equalsIgnoreCase("false"))

    println(s"=== Starting $jobName ===")
    println(s"fromDate=$fromDate toDate=$toDate (exclusive) hotDays=$hotDays seedHot=$seedHot")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val result = Try {
      val fromTs = SparkIOUtils.formatIcebergDateBoundary(fromDate)
      val toTs = SparkIOUtils.formatIcebergDateBoundary(toDate)
      val hotCutoff = today.minusDays(hotDays.toLong).toString

      val bizDf = buildBizRtFromLakehouse(spark, fromTs, toTs)
      val total = bizDf.count()
      println(s"Built $total biz_rt rows from lakehouse")
      if (total == 0) {
        println("No rows — nothing to backfill.")
      } else {
        // Cold: history older than hot window / Cold: lịch sử cũ hơn cửa sổ hot
        val coldDf = bizDf.filter(col("ReportDate") < to_date(lit(hotCutoff)))
        val coldCount = coldDf.count()
        println(s"Cold rows (ReportDate < $hotCutoff): $coldCount")
        if (coldCount > 0) {
          writeCold(spark, coldDf)
        }

        // Hot seed: fill gap Kafka retention cannot cover / Seed hot: lấp khoảng Kafka không có
        if (seedHot) {
          val hotDf = bizDf.filter(col("ReportDate") >= to_date(lit(hotCutoff)))
          val hotCount = hotDf.count()
          println(s"Hot seed rows (ReportDate >= $hotCutoff): $hotCount")
          if (hotCount > 0) {
            seedClickHouseHot(spark, config, hotDf)
          }
        }
      }
    }

    result match {
      case Success(_) =>
        println(s"✔ [$jobName] finished successfully.")
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed:")
        e.printStackTrace()
        spark.stop()
        sys.exit(1)
    }
  }

  /** Build SALE + RETURN grains aligned with Flink BizRealtimeReportJob.
    * Dựng grain SALE/RETURN cùng logic Flink.
    */
  private def buildBizRtFromLakehouse(
      spark: org.apache.spark.sql.SparkSession,
      fromTs: String,
      toTs: String
  ): DataFrame = {
    requireTable(spark, "nessie.fnd.order")
    requireTable(spark, "nessie.fnd.payment")
    requireTable(spark, "nessie.fnd.order_payment")

    val orderFnd = spark
      .table("nessie.fnd.order")
      .filter(
        col("TransactionDate") >= fromTs && col("TransactionDate") < toTs &&
          col("Status").isin("Completed", "Returned")
      )

    val dimShop =
      if (SparkIOUtils.tableExists(spark, "nessie.dw.dim_shop")) {
        spark.table("nessie.dw.dim_shop").dropDuplicates("ShopCode")
      } else {
        orderFnd.select(col("ShopCode")).dropDuplicates()
          .withColumn("ShopName", col("ShopCode"))
      }

    val paymentMethodFnd =
      if (SparkIOUtils.tableExists(spark, "nessie.fnd.payment_method")) {
        spark.table("nessie.fnd.payment_method").dropDuplicates("Id")
      } else {
        null
      }

    val paymentFnd = spark
      .table("nessie.fnd.payment")
      .filter(col("Status") === "SUCCESS")

    val orderPayment = spark.table("nessie.fnd.order_payment")

    // Transaction lines for PTTT summary / Dòng giao dịch cho tóm tắt PTTT
    val hasTx = SparkIOUtils.tableExists(spark, "nessie.fnd.transaction")
    val txFnd = if (hasTx) {
      spark
        .table("nessie.fnd.transaction")
        .select(col("PaymentId"), col("PaymentMethodId"), col("Amount"))
    } else {
      null
    }

    val hasReturnPayment =
      SparkIOUtils.tableExists(spark, "nessie.fnd.return_payment")
    val returnPayment = if (hasReturnPayment) {
      spark.table("nessie.fnd.return_payment")
    } else {
      null
    }

    val hasReturn = SparkIOUtils.tableExists(spark, "nessie.fnd.return")
    val returnFnd = if (hasReturn) {
      spark
        .table("nessie.fnd.return")
        .filter(col("CreatedDate") >= fromTs && col("CreatedDate") < toTs)
    } else {
      null
    }

    // --- SALE grain ---
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
        col("TotalPayment").cast("double").as("SalesAmount"),
        lit(0.0).as("ReturnAmount"),
        col("TotalPayment").cast("double").as("NetRevenue"),
        col("TotalPayment").cast("double").as("TotalOrderAmount"),
        lit(0.0).as("TotalReturnAmount")
      )

    val salePayments = paymentFnd
      .filter(col("PaymentType") === 1)
      .join(orderPayment, paymentFnd("Id") === orderPayment("PaymentId"), "inner")
      .select(
        orderPayment("OrderId").as("OrderId"),
        paymentFnd("Id").as("PaymentId"),
        paymentFnd("TotalPayment").cast("double").as("PayAmt")
      )

    val saleReceipts = salePayments
      .groupBy("OrderId")
      .agg(sum("PayAmt").as("TotalReceipts"))

    val saleMethodSummary =
      buildMethodSummary(salePayments, txFnd, paymentMethodFnd, withReturnId = false)

    // INNER join — only orders with SUCCESS receipt (same as Flink CoProcess paired emit)
    // INNER join — chỉ đơn đã có phiếu thu SUCCESS (giống Flink emit khi đủ cặp)
    val saleDf = saleOrders
      .join(saleReceipts, Seq("OrderId"), "inner")
      .join(saleMethodSummary, Seq("OrderId"), "left")
      .withColumn("TotalReceipts", coalesce(col("TotalReceipts"), lit(0.0)))
      .withColumn("TotalExpenditures", lit(0.0))
      .withColumn("PaymentMethodSummary", coalesce(col("PaymentMethodSummary"), lit("")))
      // Drop zero-receipt rows (orphan orders without real payment) / Bỏ đơn không có thu thực
      .filter(col("TotalReceipts") > lit(0.0))

    // --- RETURN grain ---
    val returnDf =
      if (returnFnd == null || returnPayment == null) {
        saleDf.limit(0)
      } else {
        val returnOrders = returnFnd
          .join(
            orderFnd.select(
              col("Id").as("OrdId"),
              col("ShopCode"),
              col("Status")
            ),
            returnFnd("OrderId") === col("OrdId"),
            "inner"
          )
          .join(dimShop, Seq("ShopCode"), "left")
          .select(
            to_date(returnFnd("CreatedDate")).as("ReportDate"),
            returnFnd("OrderId").as("OrderId"),
            returnFnd("Id").as("ReturnId"),
            lit("RETURN").as("DocumentType"),
            col("ShopCode"),
            coalesce(col("ShopName"), col("ShopCode"), lit("Unknown"))
              .as("ShopName"),
            lit("Returned").as("OrderStatus"),
            lit(0.0).as("SalesAmount"),
            coalesce(returnFnd("ReturnAmount"), lit(0))
              .cast("double")
              .as("ReturnAmount"),
            (lit(0.0) - coalesce(returnFnd("ReturnAmount"), lit(0)).cast("double"))
              .as("NetRevenue"),
            lit(0.0).as("TotalOrderAmount"),
            coalesce(returnFnd("ReturnAmount"), lit(0))
              .cast("double")
              .as("TotalReturnAmount")
          )

        val returnPayments = paymentFnd
          .filter(col("PaymentType") === 2)
          .join(
            returnPayment,
            paymentFnd("Id") === returnPayment("PaymentId"),
            "inner"
          )
          .select(
            returnPayment("OrderId").as("OrderId"),
            returnPayment("ReturnId").as("ReturnId"),
            paymentFnd("Id").as("PaymentId"),
            paymentFnd("TotalPayment").cast("double").as("PayAmt")
          )

        val returnSpend = returnPayments
          .groupBy("OrderId", "ReturnId")
          .agg(max("PayAmt").as("TotalExpenditures"))

        val returnMethodSummary =
          buildMethodSummary(returnPayments, txFnd, paymentMethodFnd, withReturnId = true)

        // INNER join — only returns with SUCCESS refund payment / Chỉ phiếu trả đã hoàn tiền
        returnOrders
          .join(returnSpend, Seq("OrderId", "ReturnId"), "inner")
          .join(returnMethodSummary, Seq("OrderId", "ReturnId"), "left")
          .withColumn("TotalReceipts", lit(0.0))
          .withColumn(
            "TotalExpenditures",
            coalesce(col("TotalExpenditures"), lit(0.0))
          )
          .withColumn(
            "PaymentMethodSummary",
            coalesce(col("PaymentMethodSummary"), lit(""))
          )
      }

    val combined = saleDf
      .unionByName(returnDf)
      .withColumn("NetCashFlow", col("TotalReceipts") - col("TotalExpenditures"))
      // Same formula as Flink / ProcessSrvFundReport
      .withColumn(
        "ReconciliationDiff",
        col("NetCashFlow") - (col("TotalOrderAmount") - col("TotalReturnAmount"))
      )
      .withColumn("PendingOrder", lit(0).cast(ByteType))
      .withColumn("UpdatedAt", current_timestamp())
      .select(
        col("ReportDate"),
        col("OrderId"),
        col("ReturnId"),
        col("DocumentType"),
        col("ShopCode"),
        col("ShopName"),
        col("OrderStatus"),
        col("SalesAmount"),
        col("ReturnAmount"),
        col("NetRevenue"),
        col("TotalReceipts"),
        col("TotalExpenditures"),
        col("NetCashFlow"),
        col("PaymentMethodSummary"),
        col("TotalOrderAmount"),
        col("TotalReturnAmount"),
        col("ReconciliationDiff"),
        col("PendingOrder"),
        col("UpdatedAt")
      )
      // One row per order grain (latest report date wins if duplicates)
      // Một dòng / grain đơn (giữ ReportDate mới nhất nếu trùng)
      .withColumn(
        "rn",
        row_number().over(
          org.apache.spark.sql.expressions.Window
            .partitionBy("OrderId", "ReturnId")
            .orderBy(col("ReportDate").desc, col("UpdatedAt").desc)
        )
      )
      .filter(col("rn") === 1)
      .drop("rn")

    combined
  }

  private def buildMethodSummary(
      payments: DataFrame,
      txFnd: DataFrame,
      paymentMethodFnd: DataFrame,
      withReturnId: Boolean
  ): DataFrame = {
    val keys = if (withReturnId) Seq("OrderId", "ReturnId") else Seq("OrderId")
    if (txFnd == null) {
      return payments
        .select(keys.map(col): _*)
        .dropDuplicates()
        .withColumn("PaymentMethodSummary", lit(""))
    }

    val methodName =
      if (paymentMethodFnd != null && paymentMethodFnd.columns.contains("Id")) {
        paymentMethodFnd.select(
          col("Id").as("PaymentMethodId"),
          col("Name").as("MethodName")
        )
      } else {
        null
      }

    val payKeys =
      if (withReturnId) payments.select("OrderId", "ReturnId", "PaymentId", "PayAmt")
      else payments.select("OrderId", "PaymentId", "PayAmt")

    var withMethod = payKeys
      .join(txFnd, payKeys("PaymentId") === txFnd("PaymentId"), "left")
      .withColumn("LineAmt", coalesce(col("Amount").cast("double"), col("PayAmt")))

    withMethod =
      if (methodName != null) {
        withMethod
          .join(methodName, Seq("PaymentMethodId"), "left")
          .withColumn(
            "MethodLabel",
            coalesce(
              col("MethodName"),
              concat(lit("PTTT"), col("PaymentMethodId"))
            )
          )
      } else {
        withMethod.withColumn(
          "MethodLabel",
          concat(lit("PTTT"), col("PaymentMethodId"))
        )
      }

    val groupCols =
      if (withReturnId) Seq(col("OrderId"), col("ReturnId"), col("MethodLabel"))
      else Seq(col("OrderId"), col("MethodLabel"))

    val perMethod = withMethod
      .groupBy(groupCols: _*)
      .agg(sum("LineAmt").as("MethodAmt"))
      .withColumn(
        "Part",
        concat(col("MethodLabel"), lit(":"), col("MethodAmt").cast("long"))
      )

    if (withReturnId) {
      perMethod
        .groupBy("OrderId", "ReturnId")
        .agg(concat_ws("|", collect_list(col("Part"))).as("PaymentMethodSummary"))
    } else {
      perMethod
        .groupBy("OrderId")
        .agg(concat_ws("|", collect_list(col("Part"))).as("PaymentMethodSummary"))
    }
  }

  private def writeCold(
      spark: org.apache.spark.sql.SparkSession,
      coldDf: DataFrame
  ): Unit = {
    SparkIOUtils.ensureNamespaceExists(spark, "nessie.serving")
    SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
    SparkIOUtils.writeToIceberg(spark, coldDf, StgTable, "overwrite")
    val stgDf = SparkIOUtils.readFromIceberg(spark, StgTable)
    // Full backfill replaces serving entirely — merge would keep stale orphan SALE rows
    // Backfill đầy đủ ghi đè serving — merge sẽ giữ dòng SALE cũ không còn hợp lệ
    SparkIOUtils.writeToIceberg(spark, stgDf, ServingTable, "overwrite")
    Try(SparkIOUtils.expireTableSnapshots(spark, ServingTable))
    Try(SparkIOUtils.expireTableSnapshots(spark, StgTable))
    println(s"Cold backfill written to $ServingTable")
  }

  private def seedClickHouseHot(
      spark: org.apache.spark.sql.SparkSession,
      config: SparkConfig,
      hotDf: DataFrame
  ): Unit = {
    val host = config.get("clickhouse", "host")
    val port = config.get("clickhouse", "port")
    val database = config.get("clickhouse", "database")
    val user = config.get("clickhouse", "username")
    val password = config.get("clickhouse", "password")
    val jdbcUrl = s"jdbc:clickhouse://$host:$port/$database"

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    // ReplacingMergeTree: stream rows with newer UpdatedAt win on conflict
    // Stream ghi UpdatedAt mới hơn sẽ thắng khi trùng key
    hotDf.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "report.biz_rt_order")
      .option("user", user)
      .option("password", password)
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .mode("append")
      .save()
    println("Hot seed appended to report.biz_rt_order")
  }

  private def requireTable(
      spark: org.apache.spark.sql.SparkSession,
      table: String
  ): Unit = {
    if (!SparkIOUtils.tableExists(spark, table)) {
      throw new IllegalStateException(s"Required table missing: $table")
    }
  }
}
