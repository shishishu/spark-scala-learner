package com.learner.functions
import com.learner.utils.Context

object Tutorial_07_CheckDataFrameColumnExist extends App with Context {

  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))

  val df = sparkSession
    .createDataFrame(donuts)
    .toDF(colNames = "Donut Name", "Price")
  df.show()

  val priceColumnExists = df.columns.contains("Price")
  println(s"Does price column exist = $priceColumnExists")

  val mountColumnExists = df.columns.contains("Mount")
  println(s"Does mount column exist = $mountColumnExists")

}
