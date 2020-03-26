package com.learner.functions
import com.learner.utils.Context

object Tutorial_06_DataFrameColumnArrayContains extends App with Context {

  import sparkSession.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val tagsDF = sparkSession
    .read
    .option("multiLine", true)
    .option("inferSchema", true)
    .json(path = "src/main/resources/tags_sample.json")
  tagsDF.show()
  // https://blog.csdn.net/weixin_42411818/article/details/98734464
  // multiLine 表示是否支持一条json记录拆分成多行
  // inferSchema 表示是否支持从数据中推导出schema(只读参数)

  val df = tagsDF
    .select(explode($"stackoverflow") as "stackoverflow_tags")
    .select(
      $"stackoverflow_tags.tag.id" as "id",
      $"stackoverflow_tags.tag.author" as "author",
      $"stackoverflow_tags.tag.name" as "tag_name",
      $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
      $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
    )
  df.show()

  df.select("*")
    .where(array_contains($"frameworks_name", "Play Framework"))
    .show()
}
