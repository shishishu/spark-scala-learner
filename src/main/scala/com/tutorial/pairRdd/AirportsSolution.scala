package com.tutorial.pairRdd

import com.tutorial.commons.{Context, Utils}

object AirportsSolution extends Context {

  def main(args: Array[String]): Unit ={
    val airportsRdd = sc
      .textFile("src/main/resources/airports.text")

    val airportsPairRdd = airportsRdd
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER)
        (splits(1), splits(3))
      })

    // filter
    val airportsNotInUSA = airportsPairRdd
      .filter(keyValue => keyValue._2 != "\"United States\"")

    airportsNotInUSA
      .take(10)
      .foreach(println(_))

    // mapValues
    // 对键值对每个value都应用一个函数，但是，key不会发生变化
    val mapValues = airportsPairRdd
      .mapValues(countryName => countryName.toUpperCase)

    mapValues
      .take(10)
      .foreach(println(_))

    // groupByKey
    val countryAndAirportNameAndPair = airportsRdd
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER)
        (splits(3), splits(1))
      })

    val airportsByCountry = countryAndAirportNameAndPair
      .groupByKey()

    airportsByCountry
      .collectAsMap()  // 功能和collect函数类似。该函数用于Pair RDD，最终返回Map类型的结果
      .take(10)
      .foreach(line => println(line._1 + " : " + line._2.toList))
  }
}
