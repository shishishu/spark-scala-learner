package com.tutorial.sql

import com.tutorial.commons.Context

object StackOverFlowSurvey extends Context {

  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

  def main(args: Array[String]): Unit = {

    val df = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/2016-stack-overflow-survey-responses.csv")
    df.printSchema()
    df.show(10)

    val dfSelectedColumns = df
      .select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)
    dfSelectedColumns.show(10)

    // filter
    import sparkSession.sqlContext.implicits._
    dfSelectedColumns
      .filter($"country" === "Afghanistan")
      .show(10)

    dfSelectedColumns
      .filter($"country".equalTo("Afghanistan"))
      .show(10)

    dfSelectedColumns
      .filter(dfSelectedColumns.col("country") === "Afghanistan")
      .show(10)

    val dfGrouped = dfSelectedColumns.groupBy("occupation")
    dfGrouped.count().show(10)

    dfSelectedColumns
      .filter(dfSelectedColumns.col(AGE_MIDPOINT) < 20)
      .show(10)

    dfSelectedColumns
      .orderBy(dfSelectedColumns.col(SALARY_MIDPOINT).desc)
      .show(10)

    val dfGroupByCountry = dfSelectedColumns
      .groupBy("country")

    dfGroupByCountry.avg(SALARY_MIDPOINT).show(10)

    val dfSalaryBucket = df
      .withColumn(SALARY_MIDPOINT_BUCKET, df.col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))
    dfSalaryBucket
      .select(SALARY_MIDPOINT, SALARY_MIDPOINT_BUCKET)
      .show(10)

    dfSalaryBucket.
      groupBy(SALARY_MIDPOINT_BUCKET)
      .count()
      .orderBy(SALARY_MIDPOINT_BUCKET)
      .show(10)
  }

}
