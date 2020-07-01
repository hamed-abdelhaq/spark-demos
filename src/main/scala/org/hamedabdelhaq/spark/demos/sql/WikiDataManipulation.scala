package sql

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf


object WikiDataManipulation {

  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val sqlContext = spark.sqlContext
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

    // wikiData: DaatFrame
    val wikiData = sqlContext.read.parquet("data/wiki_parquet")

    //println(wikiData.show(100));
    wikiData.show(100)

    //val col:Column = 'ddddd;
    val col = $"ddddd";
    val ddd:Column = $"ddddd"

//    val upper: String => (String s) = {
//      val str  = _.replace("dd", "dd");
//
//      return str.toUpperCase;
//    }

//    val upper => (s: Long)= {
//           val str  = donutName.replace("dd", "dd");
//      return str.toUpperCase;
//    }


    val upper = (str: String) => {
      val str1 = str.replace("{","").replace("'","").replace("}", "");
       str1.toUpperCase};
    val upperUDF = udf(upper);


    //val upperUDF = udf(upper)


    wikiData.withColumn("Cap", upperUDF('text)).show(1000)


    //wikiData.select("text").collect().foreach(println)
    wikiData


  }

}
