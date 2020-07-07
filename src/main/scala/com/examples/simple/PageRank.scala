package com.examples.simple

import com.tutorial.commons.Context

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * {{{
 * bin/run-example SparkPageRank data/mllib/pagerank_data.txt 10
 * }}}
 */

object PageRank extends Context {

  def main(args: Array[String]): Unit = {
    val iters = if (args.length > 1) args(1).toInt else 10
    val links = sc.textFile("src/main/resources/pagerank_data.txt")
      .map(line => {
        val parts = line.split("\\s")
        (parts(0), parts(1))
      })
      .distinct()
      .groupByKey()
      .cache()

    // mapValues: 对键值对每个value都应用一个函数，但是，key不会发生变化。
    var ranks = links.mapValues(v => 1.0)


    for (i <- 1 to iters){
      // case 关键字可用于创建 PartialFunction 类型的函数字面值（匿名函数）
      val contribs = links.join(ranks).values.flatMap{case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85 * _)
    }
    val output = ranks.collect()
    output.foreach(x => println(s"${x._1} has rank: ${x._2}"))
  }

}