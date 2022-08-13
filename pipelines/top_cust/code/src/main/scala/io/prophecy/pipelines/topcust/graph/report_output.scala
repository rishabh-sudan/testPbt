package io.prophecy.pipelines.topcust.graph

import io.prophecy.libs._
import io.prophecy.pipelines.topcust.config.ConfigStore._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object report_output {

  def apply(spark: SparkSession, in: DataFrame): Unit =
    in.write
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .mode("error")
      .save(
        "dbfs:/Prophecy/kiran+testpbt@prophecy.io/report-top-test-filter.csv"
      )

}
