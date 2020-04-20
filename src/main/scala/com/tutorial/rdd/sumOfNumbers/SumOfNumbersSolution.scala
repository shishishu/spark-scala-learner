package com.tutorial.rdd.sumOfNumbers

import com.tutorial.commons.Context

object SumOfNumbersSolution extends Context {

  def main(args: Array[String]): Unit ={
    val lines = sc
      .textFile("src/main/resources/prime_nums.text")

    // \\s表示   空格,回车,换行等空白符
    // +号表示一个或多个的意思
    val numbers = lines
      .flatMap(line => line.split("\\s+"))
      .filter(number => !number.isEmpty)
      .map(_.toInt)
//    lines.flatMap(line => line.split("\\s+")).foreach(println)

    val sum = numbers.reduce((x, y) => x + y)
    println("sum of prime is: " + sum)
  }

}
