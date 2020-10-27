package com.examples.simple

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import com.tutorial.commons.Context

/**
 * K-means clustering.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.clustering.KMeans.
 */

object KMeans extends Context {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length){
      val tmpDist = squaredDistance(p, centers(i))
      if (tmpDist < closest){
        closest = tmpDist
        bestIndex = i
      }
    }
    bestIndex
  }

  def main(args: Array[String]): Unit ={

    val lines = sc.textFile("src/main/resources/kmeans.txt")
    val data = lines.map(parseVector).cache()
    val K = 2
    val convergeDist = math.pow(10, -5)

    val kPoints = Array(Vector(0.0, 0.0), Vector(3.0, 3.0))
    var tmpDist = 1.0

    while (tmpDist > convergeDist){
      val closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))

      // https://stackoverflow.com/questions/30875381/using-case-in-pairrddfunctions-reducebykey
      //Scala supports multiple ways of defining anonymous functions. The "case" version is so called Pattern Matching Anonymous Functions
      val pointStats = closest.reduceByKey{case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2)}

      val newPoints = pointStats
        .map(pair => (pair._1, pair._2._1 * (1.0 / pair._2._2)))
        .collectAsMap()

      tmpDist = 0.0
      for (i <- 0 until K){
        tmpDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints){
        kPoints(newP._1) = newP._2
      }
      println(s"Finished iteration (delta = $tmpDist)")
    }

    println("Final centers:")
    kPoints.foreach(println)
    sc.stop()
  }
}
