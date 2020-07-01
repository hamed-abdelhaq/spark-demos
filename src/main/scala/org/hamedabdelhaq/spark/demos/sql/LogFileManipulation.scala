package sql

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.log4j.BasicConfigurator
//import org.apache.log4j.varia.NullAppender
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
//
//
//Class LogFileManipulation {
//  def main(args: Array[String]): Unit = {
////
////
////
////    val nullAppender = new NullAppender
////    BasicConfigurator.configure(nullAppender)
////
////    val conf = new SparkConf().
////      setMaster(args(0)).
////      setAppName("Revenue Retrieval")
////
////    // creating spark context
////
////
////    val sc = new SparkContext(conf)
////    //import sqlContext.implicits._
////
////    val spark = SparkSession
////      .builder()
////      .appName("SparkSessionZipsExample")
////      //.config("spark.sql.warehouse.dir", warehouseLocation)
////      //.enableHiveSupport()
////      .getOrCreate()
////
////
////    val textFile = sc.textFile(args(0))
////
////
////
////
////
////    val schema = new StructType()
////      .add(StructField("id", StringType, true))
////
////
////    val df = spark.read.option("header",
////      "false").textFile(args(0))
////    df.show()
////
////
////    // Creates a DataFrame having a single column named "line"
////    //val df = textFile.
////    //val dfWithoutSchema = spark.createDataFrame(textFile,schema)
////
//////    val errors = df.filter(col("line").like("%ERROR%"))
//////    // Counts all the errors
//////    println(errors.count())
//////    // Counts errors mentioning MySQL
//////    errors.filter(col("line").like("%MySQL%")).count()
//////    // Fetches the MySQL errors as an array of strings
//////    errors.filter(col("line").like("%MySQL%")).collect()
//  }
//
//}
object LogFileManipulation {


  def main(args: Array[String]): Unit = {


    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val conf = new SparkConf().
      setMaster("local").
      setAppName("Revenue Retrieval")

    // creating spark context


    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("SparkSessionZipsExample")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //.enableHiveSupport()
      .getOrCreate()



    //    val textFile = sc.textFile(args(0))
//
//
//    val schema = new StructType()
//      .add(StructField("id", StringType, true))


    val df = spark.
      read.
      option("header", "false").
      textFile("data/bigFile.txt");



    val errors = df.filter("value like \"%without%\"");

    println(errors.count())

  }
}
