package com.learner.functions
import com.learner.utils.Context

object Tutorial_10_DataFrameColumnWithUDF extends App with Context {

  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession.createDataFrame(donuts).toDF("Donut Name", "Price")

  import org.apache.spark.sql.functions._
  import sparkSession.sqlContext.implicits._

  val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match {
    case "plain donut" => Seq(100, 500)
    case "vanilla donut" => Seq(200, 400)
    case "glazed donut" => Seq(300, 600)
    case _ => Seq(150, 150)
  }
  val udfStockMinMax = udf(stockMinMax)
  // https://blog.csdn.net/dkl12/article/details/81381151
  // 注册自定义函数（通过匿名函数）
  val dfUDF = df
    .withColumn(colName = "Stock Min Max", udfStockMinMax($"Donut Name"))
  dfUDF.show()

  def stockMinMax2(donutName: String): Seq[Int] = {
    return donutName match {
      case "plain donut" => Seq(100, 500)
      case "vanilla donut" => Seq(200, 400)
      case "glazed donut" => Seq(300, 600)
      case _ => Seq(150, 150)
    }
  }
  val udfStockMinMax2 = udf(stockMinMax2 _)
  // 注册自定义函数（通过实名函数）
  val dfUDF2 = df
    .withColumn(colName = "Stock Min Max2", udfStockMinMax2($"Donut Name"))
  dfUDF2.show()

  df
    .select(col("*"), udfStockMinMax($"Donut Name") as "Stock Min Max", udfStockMinMax2($"Donut Name") as "Stock Min Max2")
    .show()

  df
    .withColumn(colName = "Stock Min Max", udfStockMinMax($"Donut Name"))
    .withColumn(colName = "Stock Min Max2", udfStockMinMax2($"Donut Name"))
    .show()

  // withColumn和select的区别：
  // 可通过withColumn的源码看出withColumn的功能是实现增加一列，或者替换一个已存在的列，
  // 他会先判断DataFrame里有没有这个列名，如果有的话就会替换掉原来的列，没有的话就用调用select方法增加一列，
  // 所以如果我们的需求是增加一列的话，两者实现的功能一样，且最终都是调用select方法，
  // 但是withColumn会提前做一些判断处理，所以withColumn的性能不如select好。
}
