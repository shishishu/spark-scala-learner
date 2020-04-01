package com.learner.dataframe
import com.learner.functions.Tutorial_14_DataFrameStringFunctions.sparkSession
import com.learner.utils.Context
import org.apache.spark.sql.functions._
import sparkSession.sqlContext.implicits._


object DataFrame_Tutorial_01 extends App with Context {

  // create dataframe from reading a CSV file
  val dfTags = sparkSession
    .read
    .option("header", true)  // header in first row
    .option("inferSchema", true)
    .csv(path = "src/main/resources/question_tags_10K.csv")
    .toDF(colNames = "id", "tag")

  dfTags.show(10)

  // print dataframe schema
  dfTags.printSchema()

  // query: select columns
  dfTags.select("id", "tag").show(10)

  // query: filter by column value
  dfTags.filter("tag == 'php'").show(10)
  // https://blog.csdn.net/supersalome/article/details/78849581
  dfTags.filter(dfTags("tag") === "php").show(10)
  dfTags.filter($"tag".equalTo("php")).show(10)

  // query: count of rows
  println(s"number of php tags = ${dfTags.filter($"tag".equalTo("php")).count()}")

  // query: SQL Like
  dfTags.filter("tag like 's%'").show(10)

  // query: multiple filter chaining
  dfTags
    .filter("tag like 's%'")
    .filter("id == 25 or id == 108")
    .show(10)
  dfTags
    .filter($"tag".like("s%"))
    .filter($"id" === 25 || $"id" === 108)
    .show(10)

  // query: SQL IN
  dfTags.filter("id in (25, 108)").show(10)

  // query: SQL Group By
  println("group by tag value")
  dfTags.groupBy("tag").count().show(10)

  // query: SQL Group By with filter
  dfTags.groupBy("tag").count().filter($"count" > 5).show(10)

  // query: SQL order by
  dfTags.groupBy("tag").count().filter($"count" > 5).orderBy(desc("count")).show(10)
  dfTags.groupBy("tag").count().filter($"count" > 5).orderBy($"count".desc).show(10)
}
