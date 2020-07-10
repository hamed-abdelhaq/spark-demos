package sql

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

import org.apache.spark.sql.functions.{lit, when}


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


    // wikiData: DataFrame
    val wikiData = sqlContext.read.parquet("data/wiki_parquet")

    // display top 10 rows in wikiData dataframe
    wikiData.show(10)



    // Here, we create a function to apply a custom transformation to a dataframe
    //It is used by passing it to 'transform' function. As a result, it generates
    // a new column 'Magic' whose values are 1s whenever $"title" starts with Magic.
    //The 'transform' function returns the resulting dataframe
    def withMagic()(df: DataFrame): DataFrame = {
      df.withColumn(
        "Magic",
        when($"title".startsWith("Magic"), lit(1)).otherwise (lit(0))
      )
    }


    println("The resulting dataframe after adding 'Magic' column.")
    wikiData
      .transform(withMagic())
      .show(10)
    //////////////////////////////////////////////////////////////////

    //System.exit(0)




    val upper = (str: String) => {
      val str1 = str.replace("{","").replace("'","").replace("}", "");
       str1.toUpperCase};
    val upperUDF = udf(upper);
    println("Adding a new column 'Cap' using scala UDF");
    wikiData.withColumn("Cap", upperUDF('text)).show(10)



    // counting distinct 'usernames'
    println("The number of distinct users is: " + wikiData.select("username").distinct().count());


    // return the number of words with capital letters in 'title'
    val getNumOfCap = (str:String) => {
      var c = 0;
      str.split(" ").foreach(f=>{
        if (f.charAt(0).isUpper)
          c = c +1
      })
      c;
    };

    val countUpper = udf(getNumOfCap);


    val xx = wikiData.withColumn("countUpper", countUpper($"title") )
    xx.select('title, $"countUpper").show(10, false);


    //wikiData.select("text").collect().foreach(println)


  }

}
