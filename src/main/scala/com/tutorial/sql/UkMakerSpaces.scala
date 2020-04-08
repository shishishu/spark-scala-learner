package com.tutorial.sql

import com.tutorial.commons.Context
import org.apache.spark.sql.functions._

object UkMakerSpaces extends Context {

  def main(args: Array[String]): Unit = {

    val markerSpace = sparkSession
      .read
      .option("header", "true")
      .csv("src/main/resources/uk-makerspaces-identifiable-data.csv")

    val postCode = sparkSession
      .read
      .option("header", "true")
      .csv("src/main/resources/uk-postcode.csv")
      .withColumn("PostCode", concat_ws("", col("Postcode"), lit(" ")))
    // https://www.cnblogs.com/itboys/p/11217815.html: Spark SQL里concat_ws和collect_set的作用
    // https://www.cnblogs.com/yy3b2007com/p/9872492.html: 使用lit()增加常量（固定值）

    println("=== Print 5 records of makerspace table ===")
    markerSpace
      .select("Name of makerspace", "Postcode")
      .show(5)

    println("=== Print 5 records of postcode table ===")
    postCode.show(5)

    val dfJoined = markerSpace
      .join(postCode, markerSpace.col("Postcode").startsWith(postCode.col("PostCode")), "left_outer")

    dfJoined.show(10)

    println(s"markerSpace shape is: row = ${markerSpace.count()}, col = ${markerSpace.columns.length}")
    println(s"postCode shape is: row = ${postCode.count()}, col = ${postCode.columns.length}")
    println(s"dfJoined shape is: row = ${dfJoined.count()}, col = ${dfJoined.columns.length}")

    println("=== Group by Region ===")
    dfJoined
      .groupBy("Region")
      .count().as("cal_count")
      .show(10)
  }

}
