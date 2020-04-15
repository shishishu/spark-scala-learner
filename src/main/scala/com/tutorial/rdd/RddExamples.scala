package com.tutorial.rdd

import com.tutorial.commons.Context
import org.apache.spark.storage.StorageLevel

object RddExamples extends Context {

  def main(args: Array[String]): Unit = {

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd = sc.parallelize(inputWords)

    // collect
    val words = wordRdd.collect()
    for(word <- words) println(s"current word is: ${word}")

    // https://blog.csdn.net/chaoshengmingyue/article/details/82021746
    // 若需要遍历RDD中元素，大可不必使用collect，可以使用foreach语句
    wordRdd.foreach(word => println(s"current word is: ${word}"))

    // take
    wordRdd.take(3).foreach(word => println(s"current word is: ${word}"))

    // count
    println("count is: " + wordRdd.count())
    val wordCountByValue= wordRdd.countByValue()
    wordCountByValue.foreach(x => println(s"${x._1} : ${x._2}"))

    // reduce
    val inputIntegers = List(1, 2, 3, 4, 5)
    val integerRdd = sc.parallelize(inputIntegers)

    val product =integerRdd.reduce((x, y) => x * y)
    println("product is: "  + product)
    val product2 = integerRdd.reduce(_*_)
    println("product2 is: " + product2)

    // persist
    // https://blog.csdn.net/dkl12/article/details/80742498/
    // Spark 持久化（cache和persist的区别）
    // 通过源码可以看出cache()是persist()的简化方式，调用persist的无参版本，也就是调用persist(StorageLevel.MEMORY_ONLY)

    integerRdd.persist(StorageLevel.MEMORY_ONLY)
    val beginTime = System.currentTimeMillis
    integerRdd.reduce((x, y) => x * y)
    integerRdd.count()
    println("time cost with persist is: " + (System.currentTimeMillis - beginTime))
  }
}
