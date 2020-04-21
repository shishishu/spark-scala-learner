package com.tutorial.pairRdd

import com.tutorial.commons.Context

object GroupByKeyVsReduceByKey extends Context {

  def main(args: Array[String]) {

    val words = List("one", "two", "two", "three", "three", "three")
    val wordsPairRdd = sc
      .parallelize(words)
      .map(word => (word, 1))

    val wordCountsWithReduceByKey = wordsPairRdd
      .reduceByKey((x, y) => x + y)
      .collect()
    println("wordCountsWithReduceByKey: " + wordCountsWithReduceByKey.toList)

    val wordCountsWithGroupByKey = wordsPairRdd
      .groupByKey()
      .mapValues(intIterable => intIterable.size)
      .collect()
    println("wordCountsWithGroupByKey: " + wordCountsWithGroupByKey.toList)

    val wordCountsWithGroupByKey2 = wordsPairRdd
      .groupByKey()
      .map(line => (line._1, line._2.sum))
      .collect()
    println("wordCountsWithGroupByKey2: " + wordCountsWithGroupByKey2.toList)
  }

}
