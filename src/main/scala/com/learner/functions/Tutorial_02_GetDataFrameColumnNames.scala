package com.learner.functions
import com.learner.utils.Context

object Tutorial_02_GetDataFrameColumnNames extends App with Context {
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF(colNames = "Donut Name", "Price")

  val columnNames: Array[String] = df.columns
  columnNames.foreach(name => println("$name"))
  // https://www.cnblogs.com/1zhk/p/4710580.html
  // foreach用于遍历集合，而map用于映射（转换）集合到另一个集合
  // https://blog.csdn.net/bowenlaw/article/details/98633292
  // $ 符具有在String 中直接拼接 字符串 和数字 等类型 。简化了字符串拼接。
}
