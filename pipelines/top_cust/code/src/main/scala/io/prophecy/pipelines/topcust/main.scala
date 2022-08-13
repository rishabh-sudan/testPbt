package io.prophecy.pipelines.topcust

import io.prophecy.libs._
import io.prophecy.pipelines.topcust.config.ConfigStore._
import io.prophecy.pipelines.topcust.config._
import io.prophecy.pipelines.topcust.udfs.UDFs._
import io.prophecy.pipelines.topcust.udfs._
import io.prophecy.pipelines.topcust.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_customers_orders = customers_orders(spark)
    val df_Filter_1         = Filter_1(spark, df_customers_orders)
    report_output(spark, df_Filter_1)
  }

  def main(args: Array[String]): Unit = {
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    spark.conf.set("prophecy.metadata.pipeline.uri", "2735/pipelines/top_cust")
    MetricsCollector.start(spark,                    "2735/pipelines/top_cust")
    apply(spark)
    MetricsCollector.end(spark)
  }

}
