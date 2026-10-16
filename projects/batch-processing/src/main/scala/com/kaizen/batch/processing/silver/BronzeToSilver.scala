package com.kaizen.batch.processing.silver

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

/** nessie.stg.<table> → nessie.fnd.<table>: MERGE by the key of tables.yaml; a stored row is only replaced by a newer one.
  * nessie.stg.<bảng> → nessie.fnd.<bảng>: MERGE theo khoá trong tables.yaml; dòng đang lưu chỉ bị thay bằng dòng mới hơn.
  */
object BronzeToSilver {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: the bronze table already holds the rows of the run
  //                   không dùng: bảng bronze đã chứa đúng các dòng của lần chạy
  // args(3): groups of tables.yaml, "all" or e.g. "master,sales" / nhóm bảng trong tables.yaml
  // args(4): optional table names, e.g. "order,order_item"; missing or empty = every table of the groups
  //          tên bảng tuỳ chọn; thiếu hoặc rỗng = mọi bảng của nhóm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("BronzeToSilver", args(0)).getOrCreate()
    process(spark, args(3), args.lift(4).getOrElse(""))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, groups: String, tableNames: String): Unit = {
    val tables = SourceTableCatalog.load().select(groups, tableNames)

    tables.foreach { table =>
      val bronzeTable = s"nessie.stg.${table.name}"
      require(SparkUtils.tableExists(spark, bronzeTable), s"$bronzeTable does not exist: run PostgresToBronze first")
      val bronze = spark.table(bronzeTable).select(table.columns.map(column => col(column.name)): _*)

      // One row per key: the latest by the modified column, or any row when the table has none
      // Một dòng mỗi khoá: dòng mới nhất theo cột sửa đổi, hoặc dòng bất kỳ nếu bảng không có cột này
      val latest = table.modifiedColumn match {
        case Some(modified) =>
          val latestFirst = Window.partitionBy(table.key.map(col): _*).orderBy(col(modified).desc_nulls_last)
          bronze.withColumn("RowNumber", row_number().over(latestFirst)).where(col("RowNumber") === 1).drop("RowNumber")
        case None => bronze.dropDuplicates(table.key)
      }

      SparkUtils.mergeInto(spark, latest, s"nessie.fnd.${table.name}", table.key, table.partition, table.modifiedColumn)
      println(s"nessie.fnd.${table.name}: merged from $bronzeTable")
    }
  }
}
