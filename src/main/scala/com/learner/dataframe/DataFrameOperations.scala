package com.learner.dataframe

import com.learner.utils.Context
import org.apache.spark.sql.Dataset

object DataFrameOperations extends App with Context{

  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", false)
    .option("inferSchema", true)
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  val dfQuestions = dfQuestionsCSV
    .filter("score > 400 and score < 410")
    .join(dfTags, "id")
    .select("owner_userid", "tag", "creation_date", "score")
    .toDF()

  dfQuestions.show(10)

  // convert df row to scala case class
  // https://www.cnblogs.com/shimingjie/p/10374451.html
  // case class被称为样例类，是一种特殊的类，常被用于模式匹配
//  case class Tag(id: String, tag: String)
  case class Tag(id: Int, tag: String)

  import sparkSession.implicits._
  val dfTagsOfTag: Dataset[Tag] = dfTags.as[Tag]
  dfTagsOfTag
    .take(10)
    .foreach(t => println(s"id = ${t.id}, tag = ${t.tag}"))

  // convert df row to scala case class using map()
  case class Question(owner_userid: Int, tag: String, creation_date: java.sql.Timestamp, score: Int)

  def toQuestion(row: org.apache.spark.sql.Row): Question = {
    // to normalize owner_userid data
    // https://www.jianshu.com/p/c7b240cabec7
    // 在Scala里Option[T]实际上是一个容器，就像数组或是List一样，你可以把他看成是一个可能有零到一个元素的List。
    val IntOf: String => Option[Int] = _ match {
      case s if s == "NA" => None
      case s => Some(s.toInt)
    }

    import java.time._
    val DateOf: String => java.sql.Timestamp = _ match {
      case s => java.sql.Timestamp.valueOf(ZonedDateTime.parse(s).toLocalDateTime)
    }

    Question(
      owner_userid = IntOf(row.getString(0)).getOrElse(-1),
      tag = row.getString(1),
      creation_date = DateOf(row.getString(2)),
      score = row.getString(3).toInt
    )
  }

  // convert each rwo into Question case class
  val dfOfQuestion: Dataset[Question] = dfQuestions.map(row => toQuestion(row))
  dfOfQuestion
    .take(10)
    .foreach(q => println(s"owner userid = ${q.owner_userid}, tag = ${q.tag}, creation_date = ${q.creation_date}, score = ${q.score}"))

  // create df from collection
  val seqTags = Seq(
    1 -> "so_java",
    1 -> "so_jsp",
    2 -> "so_erlang",
    3 -> "so_scala",
    3 -> "so_akka"
  )

  val dfMoreTags = seqTags.toDF("id", "tag")
  dfMoreTags.show(10)

  // df: union
  val dfUnionTags = dfTags
    .union(dfMoreTags)
  println(s"original df row is: ${dfTags.count()}, col is: ${dfTags.columns.length}")
  println(s"new df row is: ${dfMoreTags.count()}, col is: ${dfTags.columns.length}")
  println(s"union df row is: ${dfUnionTags.count()}, col is: ${dfTags.columns.length}")

  // df: intersection
  val dfIntersectionTags = dfMoreTags
    .intersect(dfUnionTags)
    .show(10)

  // append column using withColumn()
  import org.apache.spark.sql.functions._
  val dfSplitColumn = dfMoreTags
    .withColumn("tmp", split($"tag", "_"))
    .select(
      $"id",
      $"tag",
      $"tmp".getItem(0).as("so_prefix"),
      $"tmp".getItem(1).as("so_tag")
    ).drop("tmp")
  dfSplitColumn.show(10)

  sparkSession.close()

}
