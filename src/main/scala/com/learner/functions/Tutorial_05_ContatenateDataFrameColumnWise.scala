package com.learner.functions
import com.learner.utils.Context

object Tutorial_05_ContatenateDataFrameColumnWise extends App with Context {

  val donuts = Seq(("111","plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333","glazed donut", 2.50))
  val dfDonuts = sparkSession
    .createDataFrame(donuts)
    .toDF(colNames = "Id", "Donut Name", "Price")
  dfDonuts.show()

  val inventory = Seq(("111", 10), ("222", 20), ("333", 30))
  val dfInventory = sparkSession
    .createDataFrame(inventory)
    .toDF(colNames = "Id", "Inventory")
  dfInventory.show()

  val dfDonutsInventory = dfDonuts.join(dfInventory, Seq("Id"), "inner")
  dfDonutsInventory.show()

}
