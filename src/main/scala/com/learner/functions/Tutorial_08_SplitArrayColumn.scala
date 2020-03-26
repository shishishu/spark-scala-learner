package com.learner.functions
import com.learner.utils.Context

object Tutorial_08_SplitArrayColumn extends App with Context {

  import sparkSession.sqlContext.implicits._

  val targets = Seq(("Plain Donut", Array(1.50, 2.0)), ("Vanilla Donut", Array(2.0, 2.50)), ("Strawberry Donut", Array(2.50, 3.50)))

  val df = sparkSession
    .createDataFrame(targets)
    .toDF("Name", "Prices")

  df.show()
  df.printSchema()

  val dfSplit = df
    .select(
      $"Name",
      $"Prices"(0).as("Low Price"),
      $"Prices"(1).as("High Price")
    )

  dfSplit.show()
}
