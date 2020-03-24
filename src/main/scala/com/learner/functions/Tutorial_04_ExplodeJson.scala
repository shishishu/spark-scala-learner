package com.learner.functions
import com.learner.utils.Context

object Tutorial_04_ExplodeJson extends App with Context {

  import sparkSession.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val tagsDF = sparkSession
    .read
    .option("multiLine", true)
    .option("inferSchema", true)
    .json(path = "src/main/resources/tags_sample.json")

  tagsDF.show()

  val df = tagsDF.select(explode($"stackoverflow") as "stackoverflow_tags")
  // https://blog.csdn.net/strongyoung88/article/details/52227568
  // 使用explode展开嵌套的JSON数据
  df.printSchema()

  df.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  ).show()
}
