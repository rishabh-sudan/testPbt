package io.prophecy.pipelines.topcust.graph

import io.prophecy.libs._
import io.prophecy.pipelines.topcust.config.ConfigStore._
import io.prophecy.pipelines.topcust.udfs.UDFs._
import io.prophecy.pipelines.topcust.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Filter_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.filter(lit(true))

}
