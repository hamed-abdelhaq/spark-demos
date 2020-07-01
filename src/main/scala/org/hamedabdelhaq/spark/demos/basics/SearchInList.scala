package org.hamedabdelhaq.spark.demos.basics

import org.apache.spark.{SparkConf, SparkContext}

object SearchInList {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Revenue Retrieval")

    // creating spark context
    val sc = new SparkContext(conf)
    val data = 1 to 10000000

    /* Parallelized collections are created by calling SparkContext’s parallelize method on an existing collection
    in your driver program (a Scala Seq). The elements of the collection are copied to form a distributed dataset
    that can be operated on in parallel. For example, here is how to create a parallelized collection holding
    the numbers 1 to 10000000:
     */
    val distData = sc.parallelize(data)

    val result = distData.filter(_ < 10).collect()

    result.take(9).foreach(println)

  }

}
