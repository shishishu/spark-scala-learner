package com.examples.simple
import com.tutorial.commons.Context
import java.util.Random
import scala.math.exp

import breeze.linalg.DenseVector

object SparkLR extends Context {

  val N = 10000  // Number of data points
  val D = 10   // Number of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 10
  val rand = new Random(42)

  case class DataPoint(x: DenseVector[Double], y: Double)

  def generateData: Array[DataPoint] = {
    def generatePoint(i: Int): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D) {rand.nextGaussian + y * R}
      DataPoint(x, y)
    }
    // 返回指定长度数组，每个数组元素为指定函数的返回值，默认从 0 开始: https://www.runoob.com/scala/scala-arrays.html
    Array.tabulate(N)(generatePoint)
  }

  def main(args: Array[String]): Unit ={
    val slices = if (args.length > 0) args(0).toInt else 2
    val points = sc.parallelize(generateData, slices).cache()

    val w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println(s"inital w: $w")

    for (i <- 1 to ITERATIONS){
      println(s"on iteration: $i")
      val gradient = points.map(p => {
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }).reduce(_ + _)
      w -= gradient * 0.1
    }

    println(s"final w: $w")
    sc.stop()
  }

}
