package com.kaizen.spark.config

import java.io.File
import org.ini4j.Ini

class SparkConfig(configFilePath: String = "config.ini") extends Serializable {
  @transient private lazy val ini = new Ini(new File(configFilePath))

  def get(section: String, key: String): String = {
    ini.get(section).get(key)
  }

  def getOrElse(section: String, key: String, default: String): String = {
    Option(ini.get(section)).flatMap(s => Option(s.get(key))).getOrElse(default)
  }
}
