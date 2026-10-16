package com.kaizen.batch.utils

import com.kaizen.batch.testing.LocalSpark
import com.kaizen.batch.utils.iceberg.Partition
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

/** Schema evolution on overwrite and mergeInto: new columns are added, type changes fail.
  * Tiến hoá schema trên overwrite và mergeInto: cột mới được thêm, đổi kiểu thì thất bại.
  */
@RunWith(classOf[JUnitRunner])
class SparkUtilsSchemaEvolutionTest extends AnyFunSuite {
  private lazy val spark = LocalSpark.isolatedSession()

  private val OverwriteTable = "nessie.dw.schema_evo_overwrite"
  private val MergeTable = "nessie.dw.schema_evo_merge"

  test("overwriteTable adds a new source column") {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dw")
    val initial = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("A", 1))),
      StructType(Seq(StructField("Id", StringType), StructField("Amount", IntegerType)))
    )
    SparkUtils.overwriteTable(spark, initial, OverwriteTable)

    val evolved = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("A", 2, "extra"))),
      StructType(
        Seq(
          StructField("Id", StringType),
          StructField("Amount", IntegerType),
          StructField("Note", StringType)
        )
      )
    )
    SparkUtils.overwriteTable(spark, evolved, OverwriteTable)

    assert(spark.table(OverwriteTable).columns.toSet == Set("Id", "Amount", "Note"))
    assert(spark.table(OverwriteTable).collect().map(_.getAs[String]("Note")).toSeq == Seq("extra"))
  }

  test("mergeInto adds a new source column then merges values") {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dw")
    val initial = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("K1", 10))),
      StructType(Seq(StructField("Id", StringType), StructField("Qty", IntegerType)))
    )
    SparkUtils.mergeInto(spark, initial, MergeTable, Seq("Id"), Partition.Unpartitioned)

    val evolved = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("K1", 20, true), Row("K2", 5, false))),
      StructType(
        Seq(
          StructField("Id", StringType),
          StructField("Qty", IntegerType),
          StructField("IsActive", BooleanType)
        )
      )
    )
    SparkUtils.mergeInto(spark, evolved, MergeTable, Seq("Id"), Partition.Unpartitioned)

    val rows = spark.table(MergeTable).orderBy("Id").collect()
    assert(spark.table(MergeTable).columns.toSet == Set("Id", "Qty", "IsActive"))
    assert(rows.map(_.getAs[Int]("Qty")).toSeq == Seq(20, 5))
    assert(rows.map(_.getAs[Boolean]("IsActive")).toSeq == Seq(true, false))
  }

  test("mergeInto drops Iceberg columns absent from catalog SELECT list") {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dw")
    val table = "nessie.dw.schema_evo_merge_partial"
    val withLegacy = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("K1", 10, "centroid"))),
      StructType(
        Seq(
          StructField("Id", StringType),
          StructField("Qty", IntegerType),
          StructField("CoordMethod", StringType)
        )
      )
    )
    SparkUtils.mergeInto(spark, withLegacy, table, Seq("Id"), Partition.Unpartitioned)

    // Catalog-style source: explicit columns only / Nguồn kiểu catalog: chỉ cột khai báo
    val catalogSource = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("K1", 99), Row("K2", 5))),
      StructType(Seq(StructField("Id", StringType), StructField("Qty", IntegerType)))
    )
    SparkUtils.mergeInto(spark, catalogSource, table, Seq("Id"), Partition.Unpartitioned)

    val rows = spark.table(table).orderBy("Id").collect()
    assert(spark.table(table).columns.toSet == Set("Id", "Qty"))
    assert(rows.map(_.getAs[Int]("Qty")).toSeq == Seq(99, 5))
  }

  test("overwriteTable drops Iceberg columns absent from source") {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dw")
    val table = "nessie.dw.schema_evo_drop"
    val initial = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("A", 1, "keep-me"))),
      StructType(
        Seq(
          StructField("Id", StringType),
          StructField("Amount", IntegerType),
          StructField("CoordMethod", StringType)
        )
      )
    )
    SparkUtils.overwriteTable(spark, initial, table)

    val narrowed = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("A", 2))),
      StructType(Seq(StructField("Id", StringType), StructField("Amount", IntegerType)))
    )
    SparkUtils.overwriteTable(spark, narrowed, table)

    assert(spark.table(table).columns.toSet == Set("Id", "Amount"))
    assert(spark.table(table).collect().map(_.getAs[Int]("Amount")).toSeq == Seq(2))
  }

  test("evolveSchema rejects incompatible type changes") {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dw")
    val table = "nessie.dw.schema_evo_type_fail"
    val initial = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("X", 1))),
      StructType(Seq(StructField("Id", StringType), StructField("Amount", IntegerType)))
    )
    SparkUtils.overwriteTable(spark, initial, table)

    val wrongType = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("X", "one"))),
      StructType(Seq(StructField("Id", StringType), StructField("Amount", StringType)))
    )
    val error = intercept[IllegalArgumentException] {
      SparkUtils.evolveSchema(spark, wrongType, table)
    }
    assert(error.getMessage.contains("cannot change type"))
  }
}
