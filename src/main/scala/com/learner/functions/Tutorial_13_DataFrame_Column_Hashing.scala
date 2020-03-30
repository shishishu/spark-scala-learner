package com.learner.functions
import com.learner.utils.Context

object Tutorial_13_DataFrame_Column_Hashing extends App with Context {

  val donuts = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
  val df = sparkSession
    .createDataFrame(donuts)
    .toDF("Donut Name", "Price", "Purchase Date")

  import org.apache.spark.sql.functions._
  import sparkSession.sqlContext.implicits._

  val dfHash = df
    .withColumn("Hash", hash($"Donut Name"))
    .withColumn("MD5", md5($"Donut Name"))
    .withColumn("SHA1", sha1($"Donut Name"))
    .withColumn("SHA2", sha2($"Donut Name", numBits = 256))

  printf("Hash val in first row is: %s\n", dfHash.first().getAs[String]("Hash"))
  println("MD5 val in first row is: " + dfHash.first().getAs[String]("MD5"))
  println("SHA1 val in first row is: ".concat(dfHash.first().getAs[String]("SHA1")))
  println(s"SHA2 val in first row is: ${dfHash.first().getAs[String]("SHA2")}")
}