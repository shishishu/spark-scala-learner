package com.learner.functions
import com.learner.utils.Context

object Tutorial_09_RenameDataFrameColumn extends App with Context {

  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  val df = sparkSession.
    createDataFrame(donuts).
    toDF("Donut Name", "Price")
  df.show()

  val dfRename = df.
    withColumnRenamed(existingName = "Donut Name", newName = "Name")
  dfRename.show()
}