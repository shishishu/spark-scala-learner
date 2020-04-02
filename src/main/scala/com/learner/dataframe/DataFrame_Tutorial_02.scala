package com.learner.dataframe
import com.learner.utils.Context
import org.apache.spark.sql.functions._

object DataFrame_Tutorial_02 extends App with Context {

  import sparkSession.sqlContext.implicits._

  val dfTags = sparkSession
    .read
    .option("header", true)
    .option("inferSchema", true)
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)

  // df: data type
  val dfQuestionsCSV = sparkSession
    .read
    .option("header", true)
    .option("inferSchema", true)
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  dfQuestionsCSV.printSchema()
  dfQuestionsCSV.show(10)

  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  dfQuestions.printSchema()
  dfQuestions.show(10)

  // query: operate on a sliced df
  val dfQuestionsSubset =dfQuestions.filter("score > 400 and score < 410").toDF()
  dfQuestionsSubset.show()
  dfQuestions.filter($"score" > 400 && $"score" < 410).show()

  // query: join
  dfQuestionsSubset.join(dfTags, "id").show(10)
  dfQuestionsSubset.join(dfTags, Seq("id")).show(10)

  // query: join and select columns
  dfQuestionsSubset
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .show(10)

  // query: join on explicit columns
  dfQuestionsSubset
    .join(dfTags, dfTags("id") === dfQuestionsSubset("id"))  // two columns with name "id"
    .show(10)
  // https://www.cnblogs.com/chushiyaoyue/p/6927488.html
  // spark关于join后有重复列的问题

  // query: inner join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "inner")
    .show(10)

  // query: left outer join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "left_outer")
    .show(10)

  // query: right outer join
  dfQuestionsSubset
    .join(dfTags, Seq("id"), "right_outer")
    .show(10)

  // query: distinct
  dfTags
    .select("tag")
    .distinct()
    .show(10)

  dfTags
      .select("id", "tag")
      .distinct()
      .show(10)
  dfTags.distinct().show(10)

  sparkSession.stop()
}
