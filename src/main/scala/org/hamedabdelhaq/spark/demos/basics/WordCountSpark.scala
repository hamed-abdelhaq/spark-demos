package org.hamedabdelhaq.spark.demos.basics

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}

// Object class to include main function
object WordCountSpark {

  def main(args: Array[String]): Unit = {

    // code segment used to prevent excessive logging
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val conf = new SparkConf().
      setMaster("local").
      setAppName("Word Count")

    // creating spark context
    val sc = new SparkContext(conf)


    val text = sc.textFile("data/bigFile.txt")//args(0))
    val counts = text.flatMap(line => line.split(" ")
    ).map(word => (word,1)).reduceByKey(_+_)

    println(counts.collect.size);
  }

}
