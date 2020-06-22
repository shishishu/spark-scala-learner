package com.tutorial.advanced

import com.tutorial.commons.{Context, Utils}
import com.tutorial.advanced.UkMakerSpacesWithoutBroadcast.loadPostCodeMap

object UkMakerSpaces extends Context {

  def main(args: Array[String]) {
    // Spark详解07广播变量Broadcast：https://www.jianshu.com/p/3bd18acd2f7f
    val postCodeMap = sc.broadcast(loadPostCodeMap())

    val makerSpaceRdd = sc.textFile("src/main/resources/uk-makerspaces-identifiable-data.csv")

    val regions = makerSpaceRdd
      .filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")
      .filter(line => getPostPrefix(line).isDefined)
      .map(line => postCodeMap.value.getOrElse(getPostPrefix(line).get, "Unknown"))

    for ((region, count) <- regions.countByValue()) println(region + " : " + count)
  }

  def getPostPrefix(line: String): Option[String] = {
    val splits = line.split(Utils.COMMA_DELIMITER, -1)
    val postcode = splits(4)
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }
}
