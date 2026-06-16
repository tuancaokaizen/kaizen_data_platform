package com.kaizen.spark.config

import java.io.File
import org.ini4j.Ini

class SparkConfig(configFilePath: String = "config.ini") extends Serializable {
  @transient private lazy val ini = {
    val file = new File(configFilePath)
    if (file.exists()) {
      new Ini(file)
    } else {
      val stream = getClass.getClassLoader.getResourceAsStream(configFilePath)
      if (stream != null) {
        try {
          new Ini(stream)
        } finally {
          stream.close()
        }
      } else {
        throw new java.io.FileNotFoundException(s"Config file $configFilePath not found on disk or classpath!")
      }
    }
  }

  def get(section: String, key: String): String = {
    ini.get(section).get(key)
  }

  def getOrElse(section: String, key: String, default: String): String = {
    Option(ini.get(section)).flatMap(s => Option(s.get(key))).getOrElse(default)
  }
}
