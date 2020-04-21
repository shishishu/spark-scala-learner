package com.tutorial.pairRdd

import com.tutorial.commons.Context

object JoinOperations extends Context {

  def main(args: Array[String]) {

    val ages = sc.parallelize(List(("Tom", 29),("John", 22)))
    val addresses = sc.parallelize(List(("James", "USA"), ("John", "UK")))

    println("===inner join===")
    val join = ages
      .join(addresses)
      .foreach(println(_))

    println("===left join===")
    val leftOuterJoin = ages
      .leftOuterJoin(addresses)
      .foreach(println(_))

    println("===right join===")
    val rightOuterJoin = ages
      .rightOuterJoin(addresses)
      .foreach(println(_))

    println("===full join===")
    val fullOuterJoin = ages
      .fullOuterJoin(addresses)
      .foreach(println(_))
  }

}
