package com.kaizen.batch.utils

import java.nio.charset.StandardCharsets

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class ConfigUtilsTest extends AnyFunSuite {

  test("Spark settings keep dotted keys without quotes") {
    val prod = ConfigUtils.section("prod", "SPARK_SESSION")
    assert(prod("spark.sql.catalog.nessie") == "org.apache.iceberg.spark.SparkCatalog")
    assert(prod("spark.sql.catalog.nessie.uri") == "http://nessie-catalog-svc.compute.svc.cluster.local:19120/api/v1")
    assert(prod("spark.sql.session.timeZone") == "UTC")
    assert(ConfigUtils.section("LOCAL", "SPARK_SESSION")("spark.sql.catalog.nessie.type") == "hadoop")
  }

  test("an unknown environment fails") {
    assert(intercept[IllegalArgumentException](ConfigUtils.load("UAT")).getMessage.contains("PROD, LOCAL"))
  }

  test("a missing password variable fails with its name, never a value") {
    assume(sys.env.get("KAIZEN_POSTGRES_PASSWORD").forall(_.isEmpty))
    val error = intercept[IllegalStateException](ConfigUtils.postgres("LOCAL", "test"))
    assert(error.getMessage.contains("KAIZEN_POSTGRES_PASSWORD"))
  }

  test("conf files hold no secret") {
    ConfigUtils.Environments.foreach { environment =>
      val resource = s"common_${environment.toLowerCase}.conf"
      val text = Source.fromResource(resource)(scala.io.Codec(StandardCharsets.UTF_8)).mkString
      val lines = text.linesIterator.map(_.trim).filterNot(_.startsWith("#")).toSeq
      assert(!lines.exists(line => line.matches("(?i)\"?password\"?\\s*=.*")), s"$resource has a password")
      assert(!lines.exists(line => line.toLowerCase.contains("access.key") || line.toLowerCase.contains("secret.key")), s"$resource has an S3 key")
    }
  }
}
