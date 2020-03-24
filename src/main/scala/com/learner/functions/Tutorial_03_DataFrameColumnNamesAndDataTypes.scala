package com.learner.functions
import com.learner.utils.Context

object Tutorial_03_DataFrameColumnNamesAndDataTypes extends App with Context {
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  val (columnNames, columnDataTypes) = df.dtypes.unzip
  println(s"DataFrame column names = ${columnNames.mkString(", ")}")
  println(s"DataFrame column data types = ${columnDataTypes.mkString(", ")}")
  // https://www.jb51.net/article/163211.htm
  // 通过mkString方法把一个集合转化为一个字符串
}
