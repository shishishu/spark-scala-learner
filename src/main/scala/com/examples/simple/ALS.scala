package com.examples.simple

import com.tutorial.commons.Context
import org.apache.commons.math3.linear._

/**
 * Alternating least squares matrix factorization.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.recommendation.ALS.
 */

object ALS extends Context {

  var M = 0 // number of movies
  var U = 0 // number of users
  var F = 0 // number of features
  var ITERATIONS = 0
  var LAMBDA = 0.01 // regularization coefficient

  def generateR(): RealMatrix ={
    val mh = randomMatrix(M, F)
    val uh = randomMatrix(U, F)
    mh.multiply(uh.transpose()) // M*U
  }

  def rmse(targetR: RealMatrix, ms: Array[RealVector], us: Array[RealVector]): Double = {
    val r = new Array2DRowRealMatrix(M, U)
    for (i <- 0 until M; j <- 0 until U){
      r.setEntry(i, j, ms(i).dotProduct(us(j)))
    }
    val diffs = r.subtract(targetR)
    var sumSqs = 0.0
    for (i <- 0 until M; j <- 0 until U){
      val diff = diffs.getEntry(i, j)
      sumSqs += math.pow(diff, 2)
    }
    math.sqrt(sumSqs / (M.toDouble * U.toDouble))
  }

  def update(i: Int, m: RealVector, us: Array[RealVector], R: RealMatrix): RealVector = {
    val U = us.length
    val F = us(0).getDimension
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    for (j <- 0 until U){
      val u = us(j)
      // Add u * u^t to XtX
      XtX = XtX.add(u.outerProduct(u))
      // Add u * rating to Xty
      Xty = Xty.add(u.mapMultiply(R.getEntry(i, j)))
    }
    // Add regularization coefs to diagonal terms
    for (d <- 0 until F){
      XtX.addToEntry(d, d, LAMBDA * U)
    }
    // Solve it with Cholesky
    new CholeskyDecomposition(XtX).getSolver.solve(Xty)
  }

  def main(args: Array[String]): Unit ={

    var slices = 0
    val options = (0 to 4).map(i => if (i < args.length) Some(args(i)) else None)

    options.toArray match {
      case Array(m, u, f, iters, slices_) =>
        M = m.getOrElse("100").toInt
        U = u.getOrElse("500").toInt
        F = f.getOrElse("10").toInt
        ITERATIONS = iters.getOrElse("5").toInt
        slices = slices_.getOrElse("2").toInt
      case _ =>
        System.err.println("Usage: SparkALS [M] [U] [F] [iters] [partitions]")
        System.exit(1)
    }

    println(s"Running with M=$M, U=$U, F=$F, iters=$ITERATIONS")

    val R = generateR()
    // Initialize m and u randomly
    var ms = Array.fill(M)(randomVector(F))
    var us = Array.fill(U)(randomVector(F))

    // Iteratively update movies then users
    val Rc = sc.broadcast(R)
    var msb = sc.broadcast(ms)
    var usb = sc.broadcast(us)
    for (iter <- 0 to ITERATIONS){
      println(s"Iteration $iter:")
      ms = sc.parallelize(0 until M, slices)
          .map(i => update(i, msb.value(i), usb.value, Rc.value))
          .collect()
      msb = sc.broadcast(ms) // Re-broadcast ms because it was updated
      us = sc.parallelize(0 until U, slices)
          .map(i => update(i, usb.value(i), msb.value, Rc.value.transpose()))
          .collect()
      usb = sc.broadcast(us) // Re-broadcast us because it was updated
      println(s"RMSE = ${rmse(R, ms, us)}")
    }
    sc.stop()
  }

  private def randomVector(n: Int): RealVector = {
    // 如果fill第二个参数只写一个值的话，那么该数组的所有元素都是该值，但是如果第二个参数是一个iterator或者random，那么数组就会被赋值为它们的值。
    new ArrayRealVector(Array.fill(n)(math.random))
  }

  private def randomMatrix(rows: Int, cols: Int): RealMatrix = {
    new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))
  }

}
