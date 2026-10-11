package sql.demos

package sql.demos

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * DataFrame basics with the retail_db dataset.
 *
 * customers schema: customer_id, customer_fname, customer_lname, customer_email,
 *                   customer_password, customer_street, customer_city,
 *                   customer_state, customer_zipcode
 *
 * Tip: the demo is split into numbered sections. Uncomment a `sys.exit(0)`
 * to stop after a section while you explore it.
 */
object SparkDfDemo2 {


  def main(args: Array[String]): Unit = {

    // =====================================================================
    // 0. SparkSession
    // =====================================================================
    // SparkSession is the unified entry point of a Spark application since Spark 2.0.
    // It replaces SparkContext, SQLContext and HiveContext with a single object.
    // (The underlying SparkContext is still available as spark.sparkContext.)
    // More information: https://medium.com/@achilleus/spark-session-10d0d66d1d24
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")   // keep the console readable

    // implicits gives conversions from Scala objects (Seq, RDD) to DataFrames/Datasets
    // and enables the $"colName" syntax for columns.
    import spark.implicits._

    // =====================================================================
    // 1. Reading data and inspecting it
    // =====================================================================
    // Spark infers the schema from the JSON records.
    val df = spark.read.json("data/retail_db_json/customers")

    df.show(10, truncate = false)
    df.printSchema()
    println(s"Number of customers: ${df.count()}")
    println(s"Columns: ${df.columns.mkString(", ")}")

    // sys.exit(0)

    // =====================================================================
    // 2. Projection: select, rename, derived columns
    // =====================================================================
    // Three equivalent ways to refer to a column: "name", col("name"), $"name"
    df.select("customer_fname", "customer_lname", "customer_city").show(5)
    df.select(col("customer_fname"), $"customer_lname").show(5)

    // Derived column with withColumn + concat_ws, and renaming with alias
    val withFullName = df
      .withColumn("full_name", concat_ws(" ", $"customer_fname", $"customer_lname"))
      .select($"customer_id", $"full_name", $"customer_state".alias("state"))
    withFullName.show(5, truncate = false)

    // Conditional column: when / otherwise (like SQL CASE WHEN)
    df.withColumn("region",
        when($"customer_state" === "PR", "Puerto Rico")
          .when($"customer_state".isin("CA", "OR", "WA"), "West Coast")
          .otherwise("Other"))
      .groupBy("region").count()
      .show()

    // Dropping columns we do not need (e.g. sensitive fields)
    val publicDF = df.drop("customer_password", "customer_email")
    publicDF.printSchema()

    // sys.exit(0)

    // =====================================================================
    // 3. Filtering rows
    // =====================================================================
    // Equality uses ===, inequality uses =!=
    // count the number of customers whose first name is "Robert" and last name is "Smith"
    val filteredDF = df.filter(col("customer_fname") === "Robert" && $"customer_lname" === "Smith")
    println("Customers named \"Robert Smith\": " + filteredDF.count())

    // where() is an alias of filter(); a SQL expression string also works
    println("Customers in Texas: " + df.where("customer_state = 'TX'").count())

    // String predicates: startsWith, endsWith, contains, like, rlike (regex)
    df.filter($"customer_fname".startsWith("Mar")).select("customer_fname").distinct().show(5)
    df.filter($"customer_street".like("%Avenue%")).select("customer_street").show(5, truncate = false)
    df.filter($"customer_zipcode".rlike("^00")).select("customer_city", "customer_zipcode").show(5)

    // Multiple conditions with || and negation with !
    df.filter(($"customer_state" === "NY" || $"customer_state" === "NJ") && !($"customer_city" === "Brooklyn"))
      .select("customer_city", "customer_state")
      .show(5)

    // sys.exit(0)

    // =====================================================================
    // 4. Grouping and aggregation
    // =====================================================================
    // calculating the number of customers per city
    val groupedByCity = df.groupBy("customer_city").count()
    groupedByCity.show(10)

    // Several aggregations at once with agg()
    df.groupBy("customer_state")
      .agg(
        count("*").alias("num_customers"),
        countDistinct("customer_city").alias("num_cities"),
        min("customer_zipcode").alias("min_zip")
      )
      .orderBy(desc("num_customers"))
      .show(10)

    // Aggregation without groupBy -> one row for the whole DataFrame
    df.agg(countDistinct("customer_state").alias("states"),
      countDistinct("customer_city").alias("cities")).show()

    // Most common first names
    df.groupBy("customer_fname").count()
      .orderBy($"count".desc)
      .limit(5)
      .show()

    // Exercise-2.1:
    // 1- Show how you can save the resulting dataframe (groupedByCity) into json file(s)
    // 2- Sort the cities according to the number of customers in a descending order.

    // sys.exit(0)

    // =====================================================================
    // 5. Chaining: filter -> group -> sort
    // =====================================================================
    // print the name of the city where "Robert Smith" has the largest frequency.
    val cityWithMostFreq = filteredDF
      .groupBy("customer_city")
      .count()
      .sort($"count".desc)

    cityWithMostFreq.show(10)

    // Bring a single row back to the driver
    val top = cityWithMostFreq.first()
    println(s"City with most frequent 'Robert Smith' is ${top.getString(0)} (${top.getLong(1)} customers)")

    // explain() shows the physical plan produced by the Catalyst optimizer
    cityWithMostFreq.explain()

    // Exercise-2.2:
    // 1- Count the number of cities that have street names ending with 'Plaza'

    // sys.exit(0)

    // =====================================================================
    // 6. Same queries in SQL (temporary view)
    // =====================================================================
    // A DataFrame can be registered as a view and queried with plain SQL.
    // Both APIs compile to the same plan.
    df.createOrReplaceTempView("customers")

    spark.sql(
      """SELECT customer_city, COUNT(*) AS cnt
        |FROM customers
        |WHERE customer_fname = 'Robert' AND customer_lname = 'Smith'
        |GROUP BY customer_city
        |ORDER BY cnt DESC
        |LIMIT 5""".stripMargin).show()

    spark.sql(
      """SELECT customer_state, COUNT(DISTINCT customer_city) AS num_cities
        |FROM customers
        |GROUP BY customer_state
        |HAVING COUNT(*) > 100
        |ORDER BY num_cities DESC""".stripMargin).show(10)

    // sys.exit(0)

    // =====================================================================
    // 7. Creating small DataFrames by hand and joining
    // =====================================================================
    // toDF() (from spark.implicits) turns a local Seq of tuples into a DataFrame.
    val stateNames: DataFrame = Seq(
      ("CA", "California"),
      ("NY", "New York"),
      ("TX", "Texas"),
      ("PR", "Puerto Rico")
    ).toDF("state_code", "state_name")

    // Inner join: only customers whose state appears in stateNames
    val joined = df.join(stateNames, df("customer_state") === stateNames("state_code"), "inner")
    joined.groupBy("state_name").count().orderBy(desc("count")).show()

    // Left join keeps every customer; unmatched states get null in state_name
    val leftJoined = df.join(stateNames, df("customer_state") === stateNames("state_code"), "left")
    println("Customers with no state name in lookup: " + leftJoined.filter($"state_name".isNull).count())

    // sys.exit(0)

    // =====================================================================
    // 8. Handling missing values
    // =====================================================================
    // na.fill replaces nulls; na.drop removes rows that contain nulls.
    leftJoined
      .na.fill("Unknown", Seq("state_name"))
      .groupBy("state_name").count()
      .show()


    // sys.exit(0)

    // =====================================================================
    // 10. Writing results
    // =====================================================================
    // mode("overwrite") avoids failure if the output folder already exists.
    // coalesce(1) produces a single output file (only for small results).
    df.groupBy("customer_state").count()
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/customers_per_state_csv")

    ///////////////////////////////////
    // Practice-2.3:
    // Join two DataFrames to find the top-5 customers in terms of the number of
    // cancelled orders (order_status = "CANCELED").
    // The 'orders' DataFrame can be loaded from data/retail_db_json/orders
    // (columns: order_id, order_date, order_customer_id, order_status).
    // Hint: filter -> join -> groupBy -> orderBy -> limit (see Sections 3-7).

    spark.stop()
  }
}

