package org.hamedabdelhaq.spark.demos.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka.KafkaUtils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object KafkaConsumer {

  def main(args: Array[String]): Unit = {

//    val nullAppender = new NullAppender
//    BasicConfigurator.configure(nullAppender)
////    val kafkaParams = Map[String, Object](
////      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
////      "key.deserializer" -> classOf[StringDeserializer],
////      "value.deserializer" -> classOf[StringDeserializer],
////      "group.id" -> "use_a_separate_group_id_for_each_stream",
////      "auto.offset.reset" -> "latest",
////      "enable.auto.commit" -> (false: java.lang.Boolean)
////    )
//
//    // Create a local StreamingContext with two working thread and batch interval of 1 second.
//    // The master requires 2 cores to prevent a starvation scenario.
//
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
//
//    val stream = KafkaUtils.createStream(ssc,"localhost:2181", "ffff", Map("testing-kafka1"->1))
//    stream.print()
//
//    ssc.start()
//    ssc.awaitTermination()





    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("testing-kafka1")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val lines = stream.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    //stream.map(record => (record.key, record.value))
   // stream.print()

        ssc.start()
        ssc.awaitTermination()


  }

}
