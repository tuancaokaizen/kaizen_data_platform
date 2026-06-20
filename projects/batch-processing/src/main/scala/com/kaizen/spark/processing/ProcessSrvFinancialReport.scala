package com.kaizen.spark.processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessSrvFinancialReport {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDate = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
    val toDate = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

    val jobName = "ProcessSrvFinancialReport"
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
        s"Generating Serving Financial Reports (detail/shop/product/order) for range: $fromTs to $toTs"
      )

      val salesFact = spark
        .table("nessie.dw.fact_sales_transaction")
        .filter(
          col("TransactionDate") >= fromTs && col("TransactionDate") < toTs
        )

      val returnFact = spark
        .table("nessie.dw.fact_return_transaction")
        .filter(
          col("TransactionDate") >= fromTs && col("TransactionDate") < toTs
        )

      val cogsFact = spark
        .table("nessie.dw.fact_cogs_transaction")
        .filter(col("CreatedDate") >= fromTs && col("CreatedDate") < toTs)

      val promoFact = spark
        .table("nessie.dw.fact_promotion_cost")
        .filter(col("CreatedDate") >= fromTs && col("CreatedDate") < toTs)

      val dimShop = spark.table("nessie.dw.dim_shop")
      val dimProduct = spark.table("nessie.dw.dim_product")
      val orderFnd = spark
        .table("nessie.fnd.order")
        .filter(
          col("TransactionDate") >= fromTs && col("TransactionDate") < toTs
        )
      val returnFnd = spark
        .table("nessie.fnd.return")
        .filter(
          col("CreatedDate") >= fromTs && col("CreatedDate") < toTs
        )

      val productGrain = Seq("ReportDate", "ShopCode", "ProductId")

      // Product-grain aggregations / Gom nhóm theo ngày + shop + sản phẩm
      val salesAgg = salesFact
        .groupBy(
          to_date(col("TransactionDate")).as("ReportDate"),
          col("ShopCode"),
          col("ProductId")
        )
        .agg(
          sum(col("Quantity")).as("SalesQuantity"),
          sum(col("TotalAmount")).as("GrossRevenue")
        )

      val returnAgg = returnFact
        .groupBy(
          to_date(col("TransactionDate")).as("ReportDate"),
          col("ShopCode"),
          col("ProductId")
        )
        .agg(
          sum(col("Quantity")).as("ReturnQuantity"),
          sum(col("TotalAmount")).as("ReturnAmount")
        )

      val cogsAgg = cogsFact
        .groupBy(
          to_date(col("CreatedDate")).as("ReportDate"),
          col("ShopCode"),
          col("ProductId")
        )
        .agg(sum(col("CostAmount")).as("TotalCogs"))

      // Promotion cost at shop grain (vouchers) — allocated to products later
      // Chi phí KM theo shop — phân bổ xuống sản phẩm sau
      val promoShopAgg = promoFact
        .groupBy(
          to_date(col("CreatedDate")).as("ReportDate"),
          col("ShopCode")
        )
        .agg(sum(col("CostAmount")).as("ShopPromotionCost"))

      val detailKeys = salesAgg
        .select(productGrain.map(col(_)): _*)
        .union(returnAgg.select(productGrain.map(col(_)): _*))
        .union(cogsAgg.select(productGrain.map(col(_)): _*))
        .distinct()

      val detailBase = detailKeys
        .join(salesAgg, productGrain, "left")
        .join(returnAgg, productGrain, "left")
        .join(cogsAgg, productGrain, "left")
        .join(dimShop, Seq("ShopCode"), "left")
        .join(dimProduct, Seq("ProductId"), "left")
        .select(
          detailKeys("ReportDate"),
          detailKeys("ShopCode"),
          coalesce(dimShop("ShopName"), lit("Unknown")).as("ShopName"),
          detailKeys("ProductId"),
          coalesce(dimProduct("Name"), lit("Unknown")).as("ProductName"),
          coalesce(dimProduct("Type"), lit("Unknown")).as("ProductLineCode"),
          when(dimProduct("Type") === "medicine", lit("Thuốc"))
            .when(dimProduct("Type") === "supplement", lit("Thực phẩm chức năng"))
            .otherwise(coalesce(dimProduct("Type"), lit("Không xác định")))
            .as("ProductLineName"),
          coalesce(col("SalesQuantity"), lit(0L)).as("SalesQuantity"),
          coalesce(col("ReturnQuantity"), lit(0L)).as("ReturnQuantity"),
          coalesce(col("GrossRevenue"), lit(0.0)).as("GrossRevenue"),
          coalesce(col("ReturnAmount"), lit(0.0)).as("ReturnAmount"),
          coalesce(col("TotalCogs"), lit(0.0)).as("TotalCogs")
        )

      // Allocate shop promotion to products by gross revenue share
      // Phân bổ KM theo shop xuống SP theo tỷ lệ doanh thu gộp
      val shopGrossWindow =
        Window.partitionBy(col("ReportDate"), col("ShopCode"))

      val detailWithPromo = detailBase
        .withColumn(
          "ShopGrossRevenue",
          sum(col("GrossRevenue")).over(shopGrossWindow)
        )
        .join(promoShopAgg, Seq("ReportDate", "ShopCode"), "left")
        .withColumn(
          "TotalPromotionCost",
          when(
            col("ShopGrossRevenue") > 0,
            coalesce(col("ShopPromotionCost"), lit(0.0)) * col("GrossRevenue") / col(
              "ShopGrossRevenue"
            )
          ).otherwise(lit(0.0))
        )
        .drop("ShopGrossRevenue", "ShopPromotionCost")

      val detailReportDf = withFinancialMetrics(detailWithPromo)

      val shopReportDf = withFinancialMetrics(
        detailReportDf
          .groupBy(col("ReportDate"), col("ShopCode"), col("ShopName"))
          .agg(
            sum(col("SalesQuantity")).as("SalesQuantity"),
            sum(col("ReturnQuantity")).as("ReturnQuantity"),
            sum(col("GrossRevenue")).as("GrossRevenue"),
            sum(col("ReturnAmount")).as("ReturnAmount"),
            sum(col("TotalCogs")).as("TotalCogs"),
            sum(col("TotalPromotionCost")).as("TotalPromotionCost")
          )
      )

      val productReportDf = withFinancialMetrics(
        detailReportDf
          .groupBy(
            col("ReportDate"),
            col("ProductId"),
            col("ProductName"),
            col("ProductLineCode"),
            col("ProductLineName")
          )
          .agg(
            sum(col("SalesQuantity")).as("SalesQuantity"),
            sum(col("ReturnQuantity")).as("ReturnQuantity"),
            sum(col("GrossRevenue")).as("GrossRevenue"),
            sum(col("ReturnAmount")).as("ReturnAmount"),
            sum(col("TotalCogs")).as("TotalCogs"),
            sum(col("TotalPromotionCost")).as("TotalPromotionCost")
          )
      )

      // Order grain — SALE rows + RETURN rows (separate documents) / Grain đơn hàng
      val salesByOrder = salesFact
        .groupBy(col("OrderId"))
        .agg(
          sum(col("Quantity")).as("SalesQuantity"),
          sum(col("TotalAmount")).as("GrossRevenue")
        )

      val cogsSalesByOrder = cogsFact
        .filter(col("ReturnId").isNull)
        .groupBy(col("OrderId"))
        .agg(sum(col("CostAmount")).as("TotalCogs"))

      val promoByOrder = promoFact
        .filter(col("ReturnId").isNull)
        .groupBy(col("OrderId"))
        .agg(sum(col("CostAmount")).as("TotalPromotionCost"))

      val saleOrderBase = orderFnd
        .join(salesByOrder, orderFnd("Id") === salesByOrder("OrderId"), "inner")
        .join(cogsSalesByOrder, orderFnd("Id") === cogsSalesByOrder("OrderId"), "left")
        .join(promoByOrder, orderFnd("Id") === promoByOrder("OrderId"), "left")
        .select(
          to_date(orderFnd("TransactionDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          lit("SALE").as("DocumentType"),
          orderFnd("Id").as("OrderId"),
          lit("").as("ReturnId"),
          orderFnd("EmployeeSaleId").as("EmployeeSaleId"),
          orderFnd("Status").as("OrderStatus"),
          coalesce(salesByOrder("SalesQuantity"), lit(0L)).as("SalesQuantity"),
          lit(0L).as("ReturnQuantity"),
          coalesce(salesByOrder("GrossRevenue"), lit(0.0)).as("GrossRevenue"),
          lit(0.0).as("ReturnAmount"),
          coalesce(cogsSalesByOrder("TotalCogs"), lit(0.0)).as("TotalCogs"),
          coalesce(promoByOrder("TotalPromotionCost"), lit(0.0)).as("TotalPromotionCost")
        )

      val returnsByReturn = returnFact
        .groupBy(col("ReturnId"), col("OrderId"))
        .agg(
          sum(col("Quantity")).as("ReturnQuantity"),
          sum(col("TotalAmount")).as("ReturnAmount")
        )

      val cogsReturnByReturn = cogsFact
        .filter(col("ReturnId").isNotNull)
        .groupBy(col("ReturnId"))
        .agg(sum(col("CostAmount")).as("TotalCogs"))

      val promoByReturn = promoFact
        .filter(col("ReturnId").isNotNull)
        .groupBy(col("ReturnId"))
        .agg(sum(col("CostAmount")).as("TotalPromotionCost"))

      val returnOrderBase = returnFnd
        .join(returnsByReturn, returnFnd("Id") === returnsByReturn("ReturnId"), "inner")
        .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
        .join(cogsReturnByReturn, returnFnd("Id") === cogsReturnByReturn("ReturnId"), "left")
        .join(promoByReturn, returnFnd("Id") === promoByReturn("ReturnId"), "left")
        .select(
          to_date(returnFnd("CreatedDate")).as("ReportDate"),
          orderFnd("ShopCode").as("ShopCode"),
          lit("RETURN").as("DocumentType"),
          returnFnd("OrderId").as("OrderId"),
          returnFnd("Id").as("ReturnId"),
          orderFnd("EmployeeSaleId").as("EmployeeSaleId"),
          lit("Returned").as("OrderStatus"),
          lit(0L).as("SalesQuantity"),
          coalesce(returnsByReturn("ReturnQuantity"), lit(0L)).as("ReturnQuantity"),
          lit(0.0).as("GrossRevenue"),
          coalesce(returnsByReturn("ReturnAmount"), lit(0.0)).as("ReturnAmount"),
          coalesce(cogsReturnByReturn("TotalCogs"), lit(0.0)).as("TotalCogs"),
          coalesce(promoByReturn("TotalPromotionCost"), lit(0.0)).as("TotalPromotionCost")
        )

      val orderReportDf = withFinancialMetrics(
        saleOrderBase
          .unionByName(returnOrderBase)
          .join(
            dimShop.select(col("ShopCode"), col("ShopName")),
            Seq("ShopCode"),
            "left"
          )
          .select(
            col("ReportDate"),
            col("ShopCode"),
            coalesce(col("ShopName"), lit("Unknown")).as("ShopName"),
            col("DocumentType"),
            col("OrderId"),
            col("ReturnId"),
            col("EmployeeSaleId"),
            col("OrderStatus"),
            col("SalesQuantity"),
            col("ReturnQuantity"),
            col("GrossRevenue"),
            col("ReturnAmount"),
            col("TotalCogs"),
            col("TotalPromotionCost")
          )
      )

      writeServingTable(
        spark,
        detailReportDf,
        "nessie.serving.financial_report_detail",
        "nessie.stg.financial_report_detail",
        Seq("ReportDate", "ShopCode", "ProductId")
      )
      writeServingTable(
        spark,
        shopReportDf,
        "nessie.serving.financial_report",
        "nessie.stg.financial_report",
        Seq("ReportDate", "ShopCode")
      )
      writeServingTable(
        spark,
        productReportDf,
        "nessie.serving.financial_report_product",
        "nessie.stg.financial_report_product",
        Seq("ReportDate", "ProductId")
      )
      writeServingTable(
        spark,
        orderReportDf,
        "nessie.serving.financial_report_order",
        "nessie.stg.financial_report_order",
        Seq("DocumentType", "OrderId", "ReturnId")
      )

      println("✔ Serving Financial Reports (detail/shop/product/order) completed.")
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Seq(
          "nessie.stg.financial_report",
          "nessie.stg.financial_report_detail",
          "nessie.stg.financial_report_product",
          "nessie.stg.financial_report_order",
          "nessie.serving.financial_report",
          "nessie.serving.financial_report_detail",
          "nessie.serving.financial_report_product",
          "nessie.serving.financial_report_order"
        ).foreach(t => Try(SparkIOUtils.expireTableSnapshots(spark, t)))
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed with error:")
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }

  private def withFinancialMetrics(df: DataFrame): DataFrame = {
    df
      .withColumn(
        "NetRevenue",
        coalesce(col("GrossRevenue"), lit(0.0)) - coalesce(
          col("ReturnAmount"),
          lit(0.0)
        )
      )
      .withColumn(
        "TotalCost",
        coalesce(col("TotalCogs"), lit(0.0)) + coalesce(
          col("TotalPromotionCost"),
          lit(0.0)
        )
      )
      .withColumn(
        "GrossProfit",
        col("NetRevenue") - col("TotalCost")
      )
      .withColumn(
        "ProfitMargin",
        when(col("NetRevenue") > 0, round(col("GrossProfit") / col("NetRevenue"), 4))
          .otherwise(lit(0.0))
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
