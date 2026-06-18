package com.kaizen.spark.ingest

import org.apache.spark.sql.SparkSession
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object GenericIngestJob {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val sourceTable = parsedArgs.getOrElse(
      "sourceTable",
      throw new IllegalArgumentException("Missing parameter --sourceTable")
    )
    val targetTable = parsedArgs.getOrElse(
      "targetTable",
      throw new IllegalArgumentException("Missing parameter --targetTable")
    )
    val loadType = parsedArgs.getOrElse("loadType", "full").toLowerCase
    val watermarkColumn = parsedArgs.get("watermarkColumn")
    val joinKey = parsedArgs.get("joinKey")

    val jobName = s"GenericIngestJob-$targetTable-$loadType"
    println(s"=== Starting Ingestion Job: $jobName ===")
    println(s"Source Table: $sourceTable")
    println(s"Target Table Identifier: $targetTable")
    println(s"Load Type: $loadType")
    watermarkColumn.foreach(col => println(s"Watermark Column: $col"))
    joinKey.foreach(key => println(s"Join Key(s): $key"))

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val stgTable = s"nessie.stg.$targetTable"
    val fndTable = s"nessie.fnd.$targetTable"

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.fnd")

      loadType match {
        case "incremental" =>
          val watermarkCol = watermarkColumn.getOrElse(
            throw new IllegalArgumentException(
              "Watermark column is required for incremental load"
            )
          )
          val keysStr = joinKey.getOrElse(
            throw new IllegalArgumentException(
              "Join key is required for incremental load"
            )
          )
          val joinKeys = keysStr.split(",").map(_.trim).toSeq

          val fromDateOpt = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
          val toDateOpt = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

          if (fromDateOpt.isDefined || toDateOpt.isDefined) {
            val fromDate = fromDateOpt.getOrElse("1970-01-01")
            val toDate = toDateOpt.getOrElse(java.time.LocalDate.now().toString)
            val formattedFrom = formatTimestamp(fromDate, isEndOfDay = false)
            val formattedTo = formatTimestamp(toDate, isEndOfDay = false)
            println(
              s"[$jobName] Manual date range specified. Ingesting from $formattedFrom to $formattedTo..."
            )
            val rangeDf = SparkIOUtils.readFromPostgres(
              spark,
              sourceTable,
              config,
              Some(watermarkCol),
              Some(formattedFrom),
              Some(formattedTo)
            )

            if (rangeDf.isEmpty) {
              println(
                s"✔ [$jobName] No data found in PostgreSQL in range $formattedFrom to $formattedTo. Exiting."
              )
            } else {
              val targetExists = SparkIOUtils.tableExists(spark, fndTable)
              if (!targetExists) {
                println(
                  s"[$jobName] Target table '$fndTable' does not exist. Initializing table with range data..."
                )
                SparkIOUtils.writeToIceberg(
                  spark,
                  rangeDf,
                  stgTable,
                  "overwrite"
                )
                val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
                SparkIOUtils.writeToIceberg(spark, stgDf, fndTable, "overwrite")
              } else {
                writeStagingAndMerge(
                  spark,
                  rangeDf,
                  stgTable,
                  fndTable,
                  joinKeys
                )
              }
              println(
                s"✔ [$jobName] Incremental load for range $formattedFrom to $formattedTo completed successfully."
              )
            }
          } else {
            val targetExists = SparkIOUtils.tableExists(spark, fndTable)
            if (targetExists) {
              println(
                s"[$jobName] Target table '$fndTable' exists. Checking max watermark..."
              )
              val maxWatermarkOpt =
                SparkIOUtils.getMaxWatermark(spark, fndTable, watermarkCol)

              maxWatermarkOpt match {
                case Some(maxTs) =>
                  println(
                    s"[$jobName] Found max watermark: $maxTs. Reading incremental data from PostgreSQL..."
                  )
                  val incrementalDf = SparkIOUtils.readFromPostgres(
                    spark,
                    sourceTable,
                    config,
                    Some(watermarkCol),
                    Some(maxTs)
                  )

                  if (incrementalDf.isEmpty) {
                    println(
                      s"✔ [$jobName] No new data found in PostgreSQL since watermark $maxTs. Exiting."
                    )
                  } else {
                    writeStagingAndMerge(
                      spark,
                      incrementalDf,
                      stgTable,
                      fndTable,
                      joinKeys
                    )
                    println(
                      s"✔ [$jobName] Incremental load completed successfully. Merged new rows into $fndTable"
                    )
                  }
                case None =>
                  println(
                    s"[$jobName] Max watermark not found in '$fndTable' (empty table). Executing full load initialization..."
                  )
                  executeFullLoad(
                    spark,
                    config,
                    sourceTable,
                    stgTable,
                    fndTable,
                    jobName
                  )
              }
            } else {
              println(
                s"[$jobName] Target table '$fndTable' does not exist. Initializing table via full load..."
              )
              executeFullLoad(
                spark,
                config,
                sourceTable,
                stgTable,
                fndTable,
                jobName
              )
            }
          }

        case "full" | _ =>
          println(s"[$jobName] Executing full load ingestion...")
          executeFullLoad(
            spark,
            config,
            sourceTable,
            stgTable,
            fndTable,
            jobName
          )
      }
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] Ingestion job finished successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(spark, stgTable)
          SparkIOUtils.expireTableSnapshots(spark, fndTable)
        }
        spark.stop()
      case Failure(e) =>
        System.err.println(
          s"❌ [$jobName] failed for source table '$sourceTable' with error:"
        )
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }

  private def formatTimestamp(dateStr: String, isEndOfDay: Boolean): String = {
    val trimmed = dateStr.trim
    if (trimmed.length == 10) {
      if (isEndOfDay) s"$trimmed 23:59:59" else s"$trimmed 00:00:00"
    } else {
      trimmed
    }
  }

  private def executeFullLoad(
      spark: org.apache.spark.sql.SparkSession,
      config: SparkConfig,
      sourceTable: String,
      stgTable: String,
      fndTable: String,
      jobName: String
  ): Unit = {
    println(s"[$jobName] Reading full table '$sourceTable' from PostgreSQL...")
    val df =
      SparkIOUtils.readFromPostgres(spark, sourceTable, config, None, None)

    println(s"[$jobName] Writing to Iceberg Nessie staging table: $stgTable")
    SparkIOUtils.writeToIceberg(spark, df, stgTable, "overwrite")

    println(s"[$jobName] Reading from Iceberg Nessie staging table: $stgTable")
    val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)

    println(s"[$jobName] Writing to Iceberg Nessie foundation table: $fndTable")
    SparkIOUtils.writeToIceberg(spark, stgDf, fndTable, "overwrite")

    println(
      s"✔ [$jobName] Full load completed successfully. Tables ingested: $stgTable -> $fndTable"
    )
  }

  private def writeStagingAndMerge(
      spark: org.apache.spark.sql.SparkSession,
      df: org.apache.spark.sql.DataFrame,
      stgTable: String,
      fndTable: String,
      joinKeys: Seq[String]
  ): Unit = {
    SparkIOUtils.writeToIceberg(spark, df, stgTable, "overwrite")
    val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
    SparkIOUtils.mergeIcebergTable(spark, stgDf, fndTable, joinKeys)
  }
}
