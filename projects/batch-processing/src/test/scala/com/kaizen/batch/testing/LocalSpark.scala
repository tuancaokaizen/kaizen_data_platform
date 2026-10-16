package com.kaizen.batch.testing

import java.nio.file.Files

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** Spark of the LOCAL environment (common_local.conf) with the `nessie` catalog in a temporary folder.
  * Spark của môi trường LOCAL (common_local.conf) với catalog `nessie` trong thư mục tạm.
  */
object LocalSpark {

  lazy val session: SparkSession = {
    val warehouse = Files.createTempDirectory("kaizen-batch-warehouse").toUri.toString
    SparkUtils
      .getSpark("kaizen-batch-test", "LOCAL")
      .config("spark.sql.catalog.nessie.warehouse", warehouse)
      .getOrCreate()
  }

  /** A session on the same Spark with its own empty `nessie` catalog, so a test class never sees the rows of another.
    * Một session trên cùng Spark với catalog `nessie` rỗng riêng, để lớp test không thấy dòng của lớp khác.
    */
  def isolatedSession(): SparkSession = {
    val isolated = session.newSession()
    isolated.conf.set("spark.sql.catalog.nessie.warehouse", Files.createTempDirectory("kaizen-batch-warehouse").toUri.toString)
    isolated
  }
}
