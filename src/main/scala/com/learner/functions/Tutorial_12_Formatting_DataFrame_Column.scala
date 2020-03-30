package com.learner.functions
import com.learner.utils.Context

object Tutorial_12_Formatting_DataFrame_Column extends App with Context {

  val donuts = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price", "Purchase Date")

  import org.apache.spark.sql.functions._
  import sparkSession.sqlContext.implicits._

  df
    .withColumn(colName = "Price Formatted", format_number($"Price", 2))
    .withColumn(colName = "Name Formatted", format_string("awesome %s", $"Donut Name"))
    .withColumn(colName = "Name Uppercase", upper($"Donut Name"))
    .withColumn(colName = "Name Lowercase", lower($"Donut Name"))
    .withColumn(colName = "Date Formatted", date_format($"Purchase Date", "yyyyMMdd"))
    .withColumn(colName = "Day", dayofmonth($"Purchase Date"))
    .withColumn(colName = "Month", month($"Purchase Date"))
    .withColumn(colName = "Year", year($"Purchase Date"))
    .show()
}