package com.tutorial.rdd.nasaApacheWebLogs

import com.tutorial.commons.Context

object SameHostsSolution extends Context {

  def main(args: Array[String]): Unit ={

    val linesJul = sc.textFile("src/main/resources/nasa_19950701.tsv")
    val linesAug = sc.textFile("src/main/resources/nasa_19950801.tsv")

    val hostsJul = linesJul.map(line => line.split("\t")(0))
    val hostsAug = linesAug.map(line => line.split("\t")(0))

    val intersection = hostsJul
      .intersection(hostsAug)
      .filter(host => host != "host")

    intersection.take(10).foreach(println(_))

    val unionLogs = linesJul
      .union(linesAug)
      .filter(line => isNotHeader(line))

    val sampleLogs = unionLogs
      .sample(withReplacement = true, fraction = 0.1)

    sampleLogs.take(10).foreach(println(_))
  }

  def isNotHeader(line: String): Boolean = {
    !(line.startsWith("host") && line.contains("bytes"))
  }
}
