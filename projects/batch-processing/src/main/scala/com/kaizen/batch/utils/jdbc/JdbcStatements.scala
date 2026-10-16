package com.kaizen.batch.utils.jdbc

import java.sql.DriverManager

import org.slf4j.LoggerFactory

/** Runs statements that Spark's JDBC writer cannot (TRUNCATE, VACUUM) on the driver, in autocommit mode.
  * Chạy các câu lệnh mà JDBC writer của Spark không làm được (TRUNCATE, VACUUM) trên driver, ở chế độ autocommit.
  */
object JdbcStatements {
  private val log = LoggerFactory.getLogger(getClass)

  def execute(options: JdbcOptions, statements: Seq[String]): Unit = {
    Class.forName(options.driver)
    val connection = DriverManager.getConnection(options.url, options.user, options.password)
    try {
      // VACUUM cannot run inside a transaction / VACUUM không chạy được trong transaction
      connection.setAutoCommit(true)
      statements.foreach { statement =>
        log.info(s"Execute on ${options.url}: $statement")
        val jdbcStatement = connection.createStatement()
        try jdbcStatement.execute(statement)
        finally jdbcStatement.close()
      }
    } finally {
      connection.close()
    }
  }
}
