package com.tutorial.sql

import com.tutorial.commons.Context

object TypedDataset extends Context {

  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket"

  def main(args: Array[String]): Unit = {

    import sparkSession.implicits._

    val df = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/2016-stack-overflow-survey-responses.csv")
    df.show(10)

    val dfSelectedColumns = df
      .select("country", "occupation", AGE_MIDPOINT, SALARY_MIDPOINT)


    val typedDataset = dfSelectedColumns.as[Response]
    typedDataset.printSchema()
    typedDataset.show(10)

    // filter
    typedDataset
      .filter(response => response.country == "Afghanistan")
      .show(10)

    typedDataset
      .groupBy(typedDataset.col("occupation"))
      .count()
      .show(10)

    typedDataset
      .groupBy("occupation")
      .count()
      .show(10)

    // isDefined: https://windor.gitbooks.io/beginners-guide-to-scala/content/chp5-the-option-type.html
    typedDataset
      .filter(response => response.age_midpoint.isDefined && response.age_midpoint.get < 20.0)
      .show(10)

    import sparkSession.sqlContext.implicits._
    typedDataset
      .orderBy(typedDataset.col(SALARY_MIDPOINT).desc)
      .show(10)

    typedDataset
      .filter(response => response.salary_midpoint.isDefined)
      .groupBy("country")
      .avg(SALARY_MIDPOINT)
      .show(10)
  }

}
