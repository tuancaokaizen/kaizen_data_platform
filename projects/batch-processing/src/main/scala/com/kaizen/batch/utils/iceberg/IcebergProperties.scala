package com.kaizen.batch.utils.iceberg

/** How many snapshots a table keeps after each write.
  * Số snapshot một bảng giữ lại sau mỗi lần ghi.
  */
final case class SnapshotRetention(retainLast: Int, olderThanDays: Int)

/** Table properties applied to every lakehouse table at creation.
  * Thuộc tính gán cho mọi bảng lakehouse khi tạo.
  */
object IcebergProperties {
  val Default: Map[String, String] = Map(
    "format-version" -> "2",
    // Keep metadata.json history short / Giữ lịch sử metadata.json ngắn
    "write.metadata.delete-after-commit.enabled" -> "true",
    "write.metadata.previous-versions-max" -> "10",
    // Allow merge-schema on writeTo append/overwrite / Cho phép merge-schema khi writeTo append/overwrite
    "write.spark.accept-any-schema" -> "true",
    // Required by expire_snapshots on Nessie / Bắt buộc để expire_snapshots chạy trên Nessie
    "gc.enabled" -> "true",
    "nessie.gc.no-warning" -> "true"
  )

  def sql(properties: Map[String, String]): String =
    properties.toSeq.sortBy(_._1).map { case (key, value) => s"'$key'='$value'" }.mkString(", ")
}
