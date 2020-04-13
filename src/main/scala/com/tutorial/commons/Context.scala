package com.tutorial.commons

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

trait Context{
  lazy val sparkConf = new SparkConf()
    .setAppName("Spark Scala Learner")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  // https://blog.csdn.net/qq_35495339/article/details/98119422
  // 【Spark】 SparkSession与SparkContext
  lazy val sc = new SparkContext(sparkConf)
}