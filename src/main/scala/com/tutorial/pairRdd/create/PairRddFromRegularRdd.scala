package com.tutorial.pairRdd.create

import com.tutorial.commons.Context

object PairRddFromRegularRdd extends Context {

  def main(args: Array[String]): Unit ={

    val inputStrings = List("Lily 23", "Jack 29", "Mary 29", "James 8")
    val regularRdds = sc.parallelize(inputStrings)

    val pairRdd = regularRdds
      .map(s => (s.split(" ")(0), s.split(" ")(1)))

    pairRdd.foreach(println(_))

    // https://blog.csdn.net/DK_ing/article/details/90813201
    // 调用coalesce()时将RDD合并到比现在的分区数更少的分区中
    pairRdd
      .coalesce(1)
      .saveAsTextFile("src/main/out/pair_rdd_from_regular_rdd")

//    pairRdd
//      .saveAsTextFile("src/main/out/pair_rdd_from_regular_rdd_2")
  }

}
