package org.hamedabdelhaq.spark.demos.streaming

import org.apache.log4j.varia.NullAppender

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredSparkStreaming {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._


    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    val words = lines.as[String].flatMap(_.split(" "))

    // Split the lines into words
    //val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(1))
      .start()
    query.awaitTermination()



  }


}
