package com.tutorial.advanced

import com.tutorial.commons.{Context, Utils}

import scala.io.Source

object UkMakerSpacesWithoutBroadcast extends Context {

  def main(args: Array[String]): Unit ={
    val postCodeMap = loadPostCodeMap()
    val makerSpaceRdd = sc.textFile("src/main/resources/uk-makerspaces-identifiable-data.csv")
    val regions = makerSpaceRdd
      .filter(line => line.split(Utils.COMMA_DELIMITER, -1)(0) != "Timestamp")
      .map(line => {
        getPostPrefixes(line).filter(prefix => postCodeMap.contains(prefix))
          .map(prefix => postCodeMap(prefix))
          .headOption.getOrElse("Unknow")
      })
    for ((region, count) <- regions.countByValue()) {
      println(region + " : " + count)
    }
  }

  def loadPostCodeMap(): Map[String, String] ={
    // Scala文件操作详解：https://www.jianshu.com/p/89be118d2f88
    val source = Source.fromFile("src/main/resources/uk-postcode.csv")
    source.getLines
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER, -1)
        splits(0) -> splits(7)
      }).toMap
  }

  def getPostPrefixes(line: String): List[String] = {
    val postcode = line.split(Utils.COMMA_DELIMITER, -1)(4)
    val cleanedPostCode = postcode.replaceAll("\\s+", "")
    (1 to cleanedPostCode.length).map(i => cleanedPostCode.substring(0, i)).toList
  }

}
