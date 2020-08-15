package org.hamedabdelhaq.spark.demos.streaming

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark._
import org.apache.spark.streaming._


object WordCountFromStream {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)




    // Create a local StreamingContext with two working thread and batch interval of 3 second.
    // The master requires 2 cores to prevent a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))


    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
//    val wordCountsMapped = wordCounts.map(x=>("The word " + x._1 + " has a frequency of " + x._2))
//    wordCountsMapped.foreachRDD(x=>{x.collect().foreach(println)})
    // Print the first ten elements of each RDD generated in this DStream to the console
   wordCounts.print()
    //wordCounts.foreachRDD(x=>{println(x.id + " " +x.count())})


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate


  }

}
