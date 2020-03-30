package com.learner.functions
import com.learner.utils.Context

object Tutorial_01_CreateDataFrameFromTuple extends App with Context {
  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
  // https://stackoverflow.com/questions/10866639/difference-between-a-seq-and-a-list-in-scala/43457354#43457354
  // A Seq is an Iterable that has a defined order of elements. Sequences provide a method apply() for indexing, ranging from 0 up to the length of the sequence. Seq has many subclasses including Queue, Range, List, Stack, and LinkedList.
  // A List is a Seq that is implemented as an immutable linked list. It's best used in cases with last-in first-out (LIFO) access patterns.

  // https://www.cnblogs.com/wonglu/p/6044839.html
  // Spark与Pandas中DataFrame对比（详细）

  val df = sparkSession
    .createDataFrame(donuts)
    .toDF(colNames = "Donut Name", "Price")

  df.show()
}
