package com.kaizen.batch.utils

import scala.collection.JavaConverters._

import com.kaizen.batch.utils.jdbc.JdbcOptions
import com.typesafe.config.{Config, ConfigFactory, ConfigUtil}

/** Reads common_<environment>.conf from the jar; args(0) of every job is the environment.
  * Đọc common_<environment>.conf trong jar; args(0) của mọi job là môi trường.
  *
  * The conf files hold no secret: a password is the name of an environment variable set from a K8s Secret.
  * File conf không chứa bí mật: mật khẩu là tên biến môi trường lấy từ K8s Secret.
  */
object ConfigUtils {
  val Environments: Seq[String] = Seq("PROD", "LOCAL")

  /** Upper-case environment name, failing on an unknown one. / Tên môi trường viết hoa, báo lỗi nếu không biết. */
  def environmentName(environment: String): String = {
    val name = environment.trim.toUpperCase
    require(Environments.contains(name), s"environment must be one of ${Environments.mkString(", ")}, got '$environment'")
    name
  }

  /** Parsed common_<environment>.conf. / Nội dung common_<environment>.conf. */
  def load(environment: String): Config = {
    val resource = s"common_${environmentName(environment).toLowerCase}.conf"
    require(getClass.getClassLoader.getResource(resource) != null, s"$resource is missing from the jar")
    ConfigFactory.parseResources(resource).resolve()
  }

  /** Settings of the block <prefix>_<ENVIRONMENT>, e.g. SPARK_SESSION_PROD, as key → value.
    * Keys written in quotes ("spark.sql.catalog.nessie") come back without quotes.
    * Cấu hình của khối <prefix>_<ENVIRONMENT>, vd SPARK_SESSION_PROD, dạng khoá → giá trị.
    * Khoá viết trong ngoặc kép ("spark.sql.catalog.nessie") trả về không còn ngoặc kép.
    */
  def section(environment: String, prefix: String): Map[String, String] = {
    val block = blockOf(environment, prefix)
    block.entrySet().asScala.map { entry =>
      ConfigUtil.splitPath(entry.getKey).asScala.mkString(".") -> entry.getValue.unwrapped().toString
    }.toMap
  }

  /** Postgres information_system connection of the environment. / Kết nối Postgres information_system của môi trường. */
  def postgres(environment: String, applicationName: String): JdbcOptions = {
    val block = blockOf(environment, "POSTGRES")
    JdbcOptions.postgres(
      host = block.getString("host"),
      port = block.getInt("port"),
      database = block.getString("database"),
      user = block.getString("user"),
      password = passwordFrom(block),
      applicationName = applicationName
    )
  }

  /** ClickHouse connection of the environment. / Kết nối ClickHouse của môi trường. */
  def clickHouse(environment: String): JdbcOptions = {
    val block = blockOf(environment, "CLICKHOUSE")
    JdbcOptions.clickHouse(
      host = block.getString("host"),
      port = block.getInt("port"),
      database = block.getString("database"),
      user = block.getString("user"),
      password = passwordFrom(block)
    )
  }

  private def blockOf(environment: String, prefix: String): Config = {
    val path = s"${prefix}_${environmentName(environment)}"
    val config = load(environment)
    require(config.hasPath(path), s"$path is missing from common_${environmentName(environment).toLowerCase}.conf")
    config.getConfig(path)
  }

  // The value is read from the variable named in the conf and never logged / Giá trị đọc từ biến có tên trong conf, không bao giờ ghi log
  private def passwordFrom(block: Config): String = {
    val variable = block.getString("password_variable")
    sys.env.get(variable).filter(_.nonEmpty).getOrElse(
      throw new IllegalStateException(s"Environment variable $variable is not set: mount it from the Kubernetes Secret")
    )
  }
}
