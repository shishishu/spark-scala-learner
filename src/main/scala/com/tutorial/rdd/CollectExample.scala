package com.tutorial.rdd

import com.tutorial.commons.Context

object CollectExample extends Context {

  def main(args: Array[String]): Unit = {

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)

    val words = wordRdd.collect()
    for(word <- words) println(s"current word is: ${word}")

    // https://blog.csdn.net/chaoshengmingyue/article/details/82021746
    // 若需要遍历RDD中元素，大可不必使用collect，可以使用foreach语句
    wordRdd.foreach(word => println(s"current word is: ${word}"))
  }
}
