package com.learner.functions
import com.learner.utils.Context

object Tutorial_15_DataFrameDropNull extends App with Context {

  val donuts = Seq(("plain dont", 1.50), (null.asInstanceOf[String], 2.0), ("glazed donut", 2.50))
  val dfWithNull = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price")

  dfWithNull.show()

  val dfWithoutNull = dfWithNull.na.drop()
  // https://stackoverflow.com/questions/35477472/difference-between-na-drop-and-filtercol-isnotnull-apache-spark
  // With df.na.drop() you drop the rows containing any null or NaN values.
  //With df.filter(df.col("onlyColumnInOneColumnDataFrame").isNotNull()) you drop those rows which have null only in the column onlyColumnInOneColumnDataFrame.
  //If you would want to achieve the same thing, that would be df.na.drop(["onlyColumnInOneColumnDataFrame"]).

  dfWithoutNull.show()

}
