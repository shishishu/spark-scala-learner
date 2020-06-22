package com.tutorial.advanced

import com.tutorial.commons.{Context, Utils}

object StackOverFlowSurvey extends Context {

  def main(args: Array[String]) {
    // Spark笔记之累加器（Accumulator）: https://www.cnblogs.com/cc11001100/p/9901606.html
    val total = sc.longAccumulator
    val missingSalaryMidPoint = sc.longAccumulator
    val processedBytes = sc.longAccumulator

    val responseRDD = sc.textFile("src/main/resources/2016-stack-overflow-survey-responses.csv")
    val responseFromCanada = responseRDD
      .filter(response => {
        processedBytes.add(response.getBytes().length)
        val splits = response.split(Utils.COMMA_DELIMITER, -1)
        total.add(1)
        if (splits(14).isEmpty) {
          missingSalaryMidPoint.add(1)
        }
      splits(2) == "Canada"
    })

    println("Count of responses from Canada: " + responseFromCanada.count())
    println("Number of bytes processed: " + processedBytes.value)
    println("Total count of responses: " + total.value)
    println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value)
  }
}
