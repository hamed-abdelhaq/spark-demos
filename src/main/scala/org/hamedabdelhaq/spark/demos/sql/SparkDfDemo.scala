package sql

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession

object SparkDfDemo {

  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    // creating conf object holding spark context information
    // required in the driver application
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()


    // using spark context to read a file from the file system and create an dataframe accordingly
    val df = spark
      .read
      .json("data/retail_db_json/customers")


    // calculating the number of customers per city
    // groupedByCity: dataframe
    val groupedByCity = df
      .groupBy("customer_city")
      .count()
    groupedByCity.show(10)


    // implicits object gives implicit conversions for converting Scala objects (incl. RDDs)
    // into a Dataset, DataFrame, Columns or supporting such conversions
    import spark.implicits._



    // Practice-2.1:
    // 1- Show how you can save the resulting dataframe into json file(s)
    // 2- Sort the cities according to the number of customers in an descending order.

    ///////////////////////////////
    // count the number of customers whose first name is "Robert" and last name is "Smith"
    // filteredDF: DateSet
    val filteredDF =
      df.filter('customer_fname === "Robert" && $"customer_lname" === "Smith");
    println("count the number of customers whose first name is \"Robert\" and last name is \"Smith\"" +
      filteredDF.count())
    // Practice-2.2:
    //1- Count the number of cities that has street names ending with 'Plaza'
    //////////////////////////////


    //////////////////////////
    // print the name of the city where the name "Robert Smith" has the largest frequency.
    val cityWithMostFreq = filteredDF
      .groupBy("customer_city")
      .count()
      .sort($"count".desc)
      .select("customer_city").take(1)(0)

    println("City with most frequent 'Robert Smith' is " + cityWithMostFreq)



    ///////////////////////////////////
    // Practice-2.3:
    // Here, you need to know how to join two dataframes with each others in order to find the top-5 customers
    // in terms of the number of cancelled orders
    // Note: you need to utilize the 'order' dataframe that can be loaded from
    // data/retail_db_json/orders


  }

}
