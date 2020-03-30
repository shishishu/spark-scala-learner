package com.learner.functions
import com.learner.utils.Context

object Tutorial_11_DataFrameFirstRow extends App with Context {

  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  val firstRow = df.first()
  println(s"First row = $firstRow")

  val firstRowColumn1 = df.first().get(0)
  println(s"First row column 1 = $firstRowColumn1")

  val firstRowColumn2 = df.first().get(1)
  println(s"First row column 2 = $firstRowColumn2")

  val firstRowColumnPrice = df.first().getAs[Double](fieldName = "Price")
  println(s"First row column Price = $firstRowColumnPrice")

  val firstRowColumnName = df.first().getAs[String](fieldName = "Donut Name")
  println(s"First row column Price = $firstRowColumnName")
}
