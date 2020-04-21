package com.tutorial.pairRdd.create

import com.tutorial.commons.Context

object PairRddFromTupleList extends Context {

  def main(args: Array[String]) {

    val tuple = List(("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8))
    val pairRDD = sc.parallelize(tuple)

    pairRDD
      .coalesce(1)
      .saveAsTextFile("src/main/out/pair_rdd_from_tuple_list")
  }

}
