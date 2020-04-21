package com.tutorial.pairRdd.filter

import com.tutorial.commons.Context
import com.tutorial.commons.Utils

object AirportsNotInUsaSolution extends Context {

  def main(args: Array[String]): Unit ={
    val airportsRdd = sc
      .textFile("src/main/resources/airports.text")

    val airportsPairRdd = airportsRdd
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER)
        (splits(1), splits(3))
      })

    val airportsNotInUSA = airportsPairRdd
      .filter(keyValue => keyValue._2 != "\"United States\"")

    airportsNotInUSA
      .saveAsTextFile("src/main/out/airports_not_in_usa_pair_rdd.text")
  }
}
