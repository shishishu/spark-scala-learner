package com.tutorial.rdd.airports

import com.tutorial.commons.Context
import com.tutorial.commons.Utils

object AirportsByLatitudeSolution extends Context {

  def main(args: Array[String]): Unit ={
    val lines = sc
      .textFile("src/main/resources/airports.text")

    val airportsSpec = lines
      .filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER)
        splits(1) + ", " + splits(6)
      })

    airportsSpec
      .take(10)
      .foreach(println)

    airportsSpec
      .saveAsTextFile("src/main/out/airports_by_latitude.text")

    val airportsInUSA = lines
      .filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER)
        splits(1) + ", " + splits(2)
      })

    airportsInUSA
      .take(10)
      .foreach(println)

    airportsInUSA
      .saveAsTextFile("src/main/out/airports_in_usa.text")
  }

}
