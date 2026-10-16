package com.kaizen.batch.architecture

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters._

/** Reads the batch source code and fails the build when the job convention is broken:
  * one object per job with exactly `def main(args)` and `def process(spark, ...)`, positional arguments
  * documented above main, args(0) = environment, DataFrame API outside utils.
  * Đọc mã nguồn batch và làm build lỗi khi vi phạm quy chuẩn job:
  * mỗi job một object với đúng `def main(args)` và `def process(spark, ...)`, tham số theo vị trí
  * ghi chú phía trên main, args(0) = môi trường, dùng DataFrame API ngoài utils.
  */
@RunWith(classOf[JUnitRunner])
class ArchitectureTest extends AnyFunSuite {
  import ArchitectureTest._

  private lazy val files: Seq[SourceFile] = {
    val stream = Files.walk(root)
    try stream.iterator().asScala.filter(_.toString.endsWith(".scala")).toList
    finally stream.close()
  }.map(path => SourceFile(path, new String(Files.readAllBytes(path), StandardCharsets.UTF_8)))

  private lazy val jobs: Seq[SourceFile] = files.filter(_.text.contains("def main(args: Array[String]): Unit"))

  private val Folders = Set("utils", "config", "ingest", "processing", "publish", "maintenance")
  private val JobFolders = Set("ingest", "processing", "publish", "maintenance")

  private def violations(found: Seq[String]): Unit =
    assert(found.isEmpty, found.mkString("\n", "\n", "\n"))

  test("source files and jobs are found") {
    assert(files.size > 10)
    assert(jobs.size >= 20)
  }

  test("package matches the folder and every file is in a known folder") {
    violations(files.flatMap { file =>
      val folderPackage = ("com.kaizen.batch" +: file.relative.split('/').dropRight(1)).mkString(".")
      val wrongPackage = if (file.text.contains(s"package $folderPackage\n")) None else Some(s"${file.relative}: expected package $folderPackage")
      val unknownFolder = if (Folders.contains(file.folder)) None else Some(s"${file.relative}: not in ${Folders.mkString(", ")}")
      wrongPackage.toSeq ++ unknownFolder
    })
  }

  test("new code never uses the old com.kaizen.spark package") {
    violations(files.filter(_.text.contains("com.kaizen.spark")).map(_.relative))
  }

  test("utils and config never import jobs") {
    violations(files.filter(file => file.folder == "utils" || file.folder == "config").flatMap { file =>
      file.importsFrom(JobFolders).map(line => s"${file.relative} imports $line")
    })
  }

  test("jobs never import other jobs") {
    violations(jobs.flatMap { job =>
      job.importsFrom(JobFolders).map(line => s"${job.relative} imports $line")
    })
  }

  test("a job lives in a job folder and is named like its file") {
    violations(jobs.flatMap { job =>
      val folder = if (JobFolders.contains(job.folder)) None else Some(s"${job.relative}: jobs belong to ${JobFolders.mkString(", ")}")
      val name = if (job.text.contains(s"object ${job.className} {")) None else Some(s"${job.relative}: expected object ${job.className}")
      folder.toSeq ++ name
    })
  }

  test("a job has exactly two defs: main and process(spark, ...)") {
    violations(jobs.flatMap { job =>
      val defs = job.text.linesIterator.filter(_.startsWith("  def ")).toSeq
      val expected = defs.size == 2 && defs.head.startsWith("  def main(args: Array[String]): Unit") &&
        defs(1).startsWith("  def process(spark: SparkSession")
      if (expected) None else Some(s"${job.relative}: defs are ${defs.map(_.trim.takeWhile(_ != '(')).mkString(", ")}")
    })
  }

  test("main documents its positional arguments and opens Spark with args(0) and the object name") {
    violations(jobs.flatMap { job =>
      val documented = if (job.text.contains("// Input arguments / Tham số đầu vào:") && job.text.contains("// args(0): environment"))
        None else Some(s"${job.relative}: missing the argument comment above main")
      val session = if (job.text.contains(s"""SparkUtils.getSpark("${job.className}", args(0)).getOrCreate()"""))
        None else Some(s"""${job.relative}: main must call SparkUtils.getSpark("${job.className}", args(0))""")
      val exit = if (job.text.contains("spark.stop()") && job.text.contains("System.exit(0)"))
        None else Some(s"${job.relative}: main must stop Spark and exit")
      documented.toSeq ++ session ++ exit
    })
  }

  test("transformations use the DataFrame API: SQL text only inside utils") {
    val sqlText = Seq("spark.sql(", "createOrReplaceTempView", "selectExpr(", "expr(", ".sql(")
    violations(files.filter(_.folder != "utils").flatMap { file =>
      sqlText.filter(file.text.contains).map(term => s"${file.relative} uses $term")
    })
  }

  test("only Source and Sink objects and utils touch external systems") {
    val external = Seq("format(\"jdbc\")", "java.sql.", "org.postgresql", "com.clickhouse", "org.apache.kafka", "org.elasticsearch")
    violations(files.filterNot { file =>
      file.className.endsWith("Source") || file.className.endsWith("Sink") || file.folder == "utils"
    }.flatMap { file =>
      external.filter(file.text.contains).map(term => s"${file.relative} uses $term")
    })
  }

  test("class names have no abbreviations") {
    val banned = Set("Txn", "Doc", "Biz", "Rt", "Srv", "Dlq", "Es", "Ch", "Pg", "Props", "Cfg", "Mgr", "Util", "Cogs", "Qty", "Proc", "Tbl", "Line")
    violations(files.flatMap { file =>
      val words = file.className.split("(?<=[a-z0-9])(?=[A-Z])").toSeq
      words.filter(banned.contains).map(word => s"${file.relative}: '$word' is not allowed in a class name")
    })
  }
}

object ArchitectureTest {
  private val root: Path = Paths.get("src/main/scala/com/kaizen/batch")

  private final case class SourceFile(path: Path, text: String) {
    val relative: String = root.relativize(path).toString.replace('\\', '/')
    val folder: String = relative.split('/').head
    val className: String = path.getFileName.toString.stripSuffix(".scala")
    val imports: Seq[String] =
      text.linesIterator.map(_.trim).filter(_.startsWith("import com.kaizen.")).map(_.stripPrefix("import ")).toSeq
    def importsFrom(folders: Set[String]): Seq[String] =
      imports.filter(line => folders.exists(name => line.startsWith(s"com.kaizen.batch.$name.")))
  }
}
