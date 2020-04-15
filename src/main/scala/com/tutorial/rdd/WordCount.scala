package com.tutorial.rdd

import com.tutorial.commons.Context

object WordCount extends Context {

  def main(args: Array[String]): Unit = {
    val lines = sc
      .textFile("src/main/resources/word_count.text")
    val words = lines
      .flatMap(line => line.split(" "))
      .map(x => x.toLowerCase())

    // https://data-flair.training/forums/topic/explain-countbyvalue-operation-in-apache-spark-rdd/
    // Explain countByValue() operation in Apache Spark RDD.
    println("method 1: countByValue=============")
    val wordCounts = words.countByValue()
    for ((word, count) <- wordCounts.take(10)) println(s"${word} : ${count}")

    // https://www.cnblogs.com/zhangrui153169/p/11375643.html
    // Spark中reduceByKey(_+_)的说明
    println("method 2: reduceByKey=============")
    val wordCounts2 = words
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
    wordCounts2
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    val wordCounts22 = words
      .map(x => (x, 1))
      .reduceByKey(_+_)
    // https://www.itread01.com/content/1546501518.html
    // Spark取出(Key,Value)型資料中Value值為前n條資料
    wordCounts22
      .sortBy(_._2, false)
      .take(10)
      .foreach(println)

    // https://www.cnblogs.com/zzhangyuhang/p/9001523.html
    // 虽然groupByKey().map(func)也能实现reduceByKey(func)功能，但是，优先使用reduceByKey(func)
    println("method 3: groupByKey + map===========")
    val wordCounts3 = words
      .map(x => (x, 1))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
    wordCounts3
      .sortBy(_._2, false)
      .take(10)
      .foreach(println)
  }
}
