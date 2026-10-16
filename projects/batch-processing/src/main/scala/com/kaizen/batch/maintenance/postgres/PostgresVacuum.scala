package com.kaizen.batch.maintenance.postgres

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.jdbc.JdbcStatements
import com.kaizen.batch.utils.{ConfigUtils, SparkUtils}
import org.apache.spark.sql.SparkSession

/** VACUUM (ANALYZE) on the Postgres tables that the streaming jobs update all day, to clear dead rows.
  * Never VACUUM FULL, which locks the table. Spark only opens the session; the work runs on the driver over JDBC,
  * so Airflow submits it with spark.master local[1].
  * VACUUM (ANALYZE) các bảng Postgres mà job streaming cập nhật cả ngày, để dọn dòng chết.
  * Không bao giờ VACUUM FULL vì khoá bảng. Spark chỉ mở session; việc chạy trên driver qua JDBC,
  * nên Airflow submit với spark.master local[1].
  */
object PostgresVacuum {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused / không dùng
  // args(3): optional table names of tables.yaml, e.g. "order,order_item"; missing or empty = the streaming tables
  //          tên bảng tuỳ chọn trong tables.yaml; thiếu hoặc rỗng = các bảng streaming
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("PostgresVacuum", args(0)).getOrCreate()
    process(spark, args(0), args.lift(3).getOrElse(""))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, environment: String, tableNames: String): Unit = {
    val streamingTables = Seq("order", "order_item", "return", "return_item", "payment", "transaction")
    val requested = SourceTableCatalog.splitList(tableNames)
    val catalog = SourceTableCatalog.load()
    val tables = (if (requested.isEmpty) streamingTables else requested).map(catalog.table)

    JdbcStatements.execute(
      ConfigUtils.postgres(environment, s"kaizen-batch-PostgresVacuum-${spark.sparkContext.applicationId}"),
      tables.map(table => s"VACUUM (ANALYZE) ${table.postgresName}")
    )
  }
}
