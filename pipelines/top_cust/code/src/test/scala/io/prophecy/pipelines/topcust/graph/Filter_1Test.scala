package io.prophecy.pipelines.topcust.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.pipelines.topcust.config._
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.math.BigDecimal

@RunWith(classOf[JUnitRunner])
class Filter_1Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Unit Test 0") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/topcust/graph/Filter_1/in/schema.json",
      "/data/io/prophecy/pipelines/topcust/graph/Filter_1/in/data/unit_test_0.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/topcust/graph/Filter_1/out/schema.json",
      "/data/io/prophecy/pipelines/topcust/graph/Filter_1/out/data/unit_test_0.json",
      "out"
    )

    val dfOutComputed =
      io.prophecy.pipelines.topcust.graph.Filter_1(spark, dfIn)
    val res = assertDFEquals(dfOut.select("customer_id"),
                             dfOutComputed.select("customer_id"),
                             maxUnequalRowsToShow,
                             1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  override def beforeAll() = {
    super.beforeAll()
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")

    val fabricName = System.getProperty("fabric")

    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(
      Array("--confFile",
            getClass.getResource(s"/config/${fabricName}.json").getPath
      )
    )
  }

}
