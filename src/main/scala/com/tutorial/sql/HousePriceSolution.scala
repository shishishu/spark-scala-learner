package com.tutorial.sql

import com.tutorial.commons.Context
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

object HousePriceSolution extends Context {

  def main(args: Array[String]): Unit = {

    val dfHousePrice = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/RealEstate.csv")
      .toDF()

    dfHousePrice.printSchema()

    dfHousePrice.show(10)

    // aggregate
    import sparkSession.implicits._  //导入这个为了隐式转换，或RDD转DataFrame之用
    dfHousePrice
      .groupBy("Location")
      .agg(avg("Price SQ Ft").as("avg_price"))
      .withColumn("avg_price_2", format_number($"avg_price", 2))
      .orderBy(asc("avg_price"))
      .show()
  }

}
