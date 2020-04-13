package com.tutorial.sql

import com.tutorial.commons.Context
import com.tutorial.commons.Utils

object RddDatasetConversion extends Context {

  def main(args: Array[String]): Unit = {
    // https://www.jianshu.com/p/d2f838894d72
    // Spark-RDD介绍
    val lines = sc
      .textFile("src/main/resources/2016-stack-overflow-survey-responses.csv")

    // https://blog.csdn.net/stardhb/article/details/50686833
    // Scala中的String.split函数
    val responseRDD = lines
      .filter(line => !line.split(Utils.COMMA_DELIMITER, -1)(2).equals("country"))  // skip header
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER, -1)
        Response(splits(2), splits(9), toInt(splits(14)), toInt(splits(6)))  // keep order
      })

    import sparkSession.implicits._
    val responseDataset = responseRDD.toDS()
    responseDataset.printSchema()
    responseDataset.show(10)

    for (response <- responseDataset.rdd.collect()) println(response)

    responseDataset.rdd.collect().foreach(println)
  }

  def toInt(split: String): Option[Double] = {
    if (split.isEmpty) None else Some(split.toDouble)
  }
}
