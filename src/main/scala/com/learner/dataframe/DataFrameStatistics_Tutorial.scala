package com.learner.dataframe
import com.learner.utils.Context
import org.apache.spark.sql.functions._

object DataFrameStatistics_Tutorial extends App with Context {

  import sparkSession.sqlContext.implicits._

  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

  // avg
  dfQuestions
    .select(avg($"score"))
    .show()
  dfQuestions
    .agg(avg($"score").as("avg_score"))
    .show()

  // max, min, mean, sum
  dfQuestions
    .select(max($"score"))
    .show()
  dfQuestions
    .select(min($"score"))
    .show()
  dfQuestions
    .select(mean($"score"))
    .show()
  dfQuestions
    .select(sum($"score"))
    .show()

  // groupby
  dfQuestions
    .filter("id > 400 and id < 450")
    .filter("owner_userid is not null")
    .join(dfTags, dfQuestions("id").equalTo(dfTags("id")))
    .groupBy(dfQuestions("owner_userid"))
    .agg(avg("score"), max("answer_count"))
    .show()
  dfQuestions
    .filter($"id" > 400 && $"id" < 450)
    .filter($"owner_userid".isNotNull)
    .join(dfTags, Seq("id"))
    .groupBy("owner_userid")
    .agg(avg("score"), max("answer_count"))
    .show()

  // describe
  dfQuestions.describe().show()

  // correlation
  val correlation = dfQuestions.stat.corr("score", "answer_count")
  println(s"correlation between column score and answer_count = $correlation")

  // covariance
  val covariance = dfQuestions.stat.cov("score", "answer_count")
  println(s"covariance between column score and answer_count = $covariance")

  // frequent items
  val dfFrequentScore = dfQuestions.stat.freqItems(Seq("answer_count"))
  dfFrequentScore.show()
  dfQuestions.stat.freqItems(Seq("answer_count"), 0.1).show()
  // https://www.cnblogs.com/ShyPeanut/p/10932802.html
  // freqItems用于计算一列或几列中出现频繁的的值的集合

  // crosstab
  val dfScoreByUserid = dfQuestions
    .filter("owner_userid > 0 and owner_userid < 20")
    .stat
    .crosstab("score", "owner_userid")
    .show(10)

  val dfQuestionsByAnswerCount = dfQuestions
    .filter("owner_userid > 0")
    .filter("answer_count in (5, 10, 20)")
  dfQuestionsByAnswerCount
    .groupBy("answer_count")
    .count()
    .show()

  dfQuestions
    .filter($"owner_userid" > 0)
    .filter($"answer_count".isin(5, 10, 20))
    .groupBy("answer_count")
    .count()
    .show()

  val cnts = List(5, 10, 20)
  dfQuestions
    .filter($"owner_userid" > 0)
    .filter($"answer_count".isin(cnts: _*))
    .groupBy("answer_count")
    .count()
    .show()
  // https://stackoverflow.com/questions/32551919/how-to-use-column-isin-with-list

  // Create a fraction map where we are only interested:
  // - 50% of the rows that have answer_count = 5
  // - 10% of the rows that have answer_count = 10
  // - 100% of the rows that have answer_count = 20
  // Note also that fractions should be in the range [0, 1]
  val fractionKeyMap = Map(5 -> 0.5, 10 -> 0.1, 20 -> 1.0)

  // Stratified sample using the fractionKeyMap
  dfQuestionsByAnswerCount
    .stat
    .sampleBy("answer_count", fractionKeyMap, 7L)
    .groupBy("answer_count")
    .count()
    .show()

  dfQuestionsByAnswerCount
    .stat
    .sampleBy("answer_count", fractionKeyMap, 37L)  // change random seed
    .groupBy("answer_count")
    .count()
    .show()

  sparkSession.close()
}
