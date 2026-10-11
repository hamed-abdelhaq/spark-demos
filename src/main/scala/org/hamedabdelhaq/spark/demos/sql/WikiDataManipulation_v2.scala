package sql.demos

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Column manipulation, custom transformations, UDFs, SQL and Datasets
 * on a Wikipedia sample stored as Parquet.
 *
 * wiki_parquet schema: id, title, modified (Unix seconds), text, username
 *
 * Tip: the demo is split into numbered sections. Uncomment a `sys.exit(0)`
 * to stop after a section while you explore it.
 */
object WikiDataManipulation_v2 {

  def main(args: Array[String]): Unit = {

    // =====================================================================
    // 0. SparkSession
    // =====================================================================
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Wiki data manipulation")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")   // hide INFO logs

    // spark.implicits._ gives us:
    //   Seq(...).toDF("a","b")      -> local collections to DataFrames
    //   $"col"                      -> Column objects
    //   Seq(MyCase(...)).toDS()     -> automatic encoders for Datasets
    //   df.filter($"age" > 18)      -> rich column expressions
    import spark.implicits._

    // =====================================================================
    // 1. Reading Parquet
    // =====================================================================
    // Parquet is columnar and stores its own schema, so no inference is needed.
    // (spark.read replaces the legacy spark.sqlContext.read.)
    val wikiData = spark.read.parquet("data/wiki_parquet")

    wikiData.printSchema()
    wikiData.show(10)
    println(s"Number of articles: ${wikiData.count()}")

    // sys.exit(0)

    // =====================================================================
    // 2. Simple column expressions
    // =====================================================================
    // Arithmetic on a column, with a new name
    wikiData.select($"id", ($"id" + 1).as("incremented"), $"title").show(10)

    // Keep only articles whose id is greater than 12401596
    wikiData.filter($"id" > 12401596).select("id", "title").show(10)

    // Convert the Unix timestamp to a readable date
    wikiData
      .select($"title", from_unixtime($"modified").as("modified_at"))
      .show(10, truncate = false)

    // Length of the title and number of words in the text
    wikiData
      .select(
        $"title",
        length($"title").as("title_length"),
        size(split($"text", "\\s+")).as("word_count"))
      .orderBy(desc("word_count"))
      .show(10, truncate = false)

    // sys.exit(0)

    // =====================================================================
    // 3. Custom transformations with transform()
    // =====================================================================
    // A transformation is a function DataFrame => DataFrame.
    // transform() lets us chain such functions like built-in methods.

    // Adds a column "Magic": 1 if the title starts with "Magic", otherwise 0
    def withMagic(df: DataFrame): DataFrame =
      df.withColumn("Magic", when($"title".startsWith("Magic"), 1).otherwise(0))

    // A parameterised transformation: flags titles that contain a given word
    def withKeywordFlag(word: String)(df: DataFrame): DataFrame =
      df.withColumn(s"has_$word", $"title".contains(word))

    // Adds the title length
    def withTitleLength(df: DataFrame): DataFrame =
      df.withColumn("title_length", length($"title"))

    println("After adding 'Magic', 'has_History' and 'title_length' columns:")
    val newDF = wikiData
      .transform(withMagic)
      .transform(withKeywordFlag("History"))
      .transform(withTitleLength)

    newDF.select("title", "Magic", "has_History", "title_length").show(10, truncate = false)
    println(s"Titles starting with 'Magic': ${newDF.filter($"Magic" === 1).count()}")

    // sys.exit(0)

    // =====================================================================
    // 4. User-defined functions (UDFs)
    // =====================================================================
    // A UDF wraps an ordinary Scala function so it can be applied to a column.
    // Always guard against null: Spark passes null for missing values.

    // 4a. Remove { ' } characters and convert to upper case
    val cleanUpper = (str: String) =>
      if (str == null) null
      else str.replace("{", "").replace("'", "").replace("}", "").toUpperCase

    val upperUDF = udf(cleanUpper)
    println("Adding a new column 'cleanedText' using a Scala UDF")
    wikiData.withColumn("cleanedText", upperUDF($"text"))
      .select("title", "cleanedText")
      .show(10)

    // 4b. Number of words in the title that start with a capital letter
    val countCapitalWords = (str: String) =>
      if (str == null) 0
      else str.split("\\s+").count(w => w.nonEmpty && w.charAt(0).isUpper)

    val noOfCap = udf(countCapitalWords)
    wikiData.withColumn("noOfCap", noOfCap($"title"))
      .select("title", "noOfCap")
      .show(10, truncate = false)

    // 4c. A UDF with two input columns: does the title appear inside the text?
    val titleInText = udf((title: String, text: String) =>
      title != null && text != null && text.toLowerCase.contains(title.toLowerCase))

    println("Articles whose text mentions their own title: " +
      wikiData.filter(titleInText($"title", $"text")).count())

    // 4d. Prefer built-in functions when they exist.
    // Built-ins are optimised by Catalyst; a UDF is a black box to the optimiser.
    // The built-in equivalent of 4a:
    wikiData
      .withColumn("cleanedText", upper(regexp_replace($"text", "[{}']", "")))
      .select("title", "cleanedText")
      .show(10)

    // sys.exit(0)

    // =====================================================================
    // 5. Distinct values and aggregation
    // =====================================================================
    // Counts unique usernames in the dataset
    println("The number of distinct users is: " + wikiData.select("username").distinct().count())

    // Most active editors
    wikiData.groupBy("username")
      .agg(count("*").as("edits"),
           max(from_unixtime($"modified")).as("last_edit"))
      .orderBy(desc("edits"))
      .show(10, truncate = false)

    // Articles with no username (anonymous edits)
    println("Articles without a username: " + wikiData.filter($"username".isNull).count())

    // sys.exit(0)

    // =====================================================================
    // 6. Spark SQL
    // =====================================================================
    // Register the DataFrame as a temporary view and query it with SQL.
    wikiData.createOrReplaceTempView("wikiTable")

    spark.sql("SELECT id, title, username FROM wikiTable LIMIT 20").show()

    spark.sql(
      """SELECT username, COUNT(*) AS edits
        |FROM wikiTable
        |WHERE username IS NOT NULL
        |GROUP BY username
        |ORDER BY edits DESC
        |LIMIT 10""".stripMargin).show()

    // A UDF can also be registered for use inside SQL
    spark.udf.register("countCapitalWords", countCapitalWords)
    spark.sql(
      """SELECT title, countCapitalWords(title) AS noOfCap
        |FROM wikiTable
        |ORDER BY noOfCap DESC
        |LIMIT 10""".stripMargin).show(truncate = false)

    // sys.exit(0)

    // =====================================================================
    // 7. Typed Datasets
    // =====================================================================
    // wikiData : Dataset[Row]           (untyped, columns checked at run time)
    // wikiDS   : Dataset[WikiDataClass1] (typed, fields checked at compile time)
    val wikiDS = wikiData.as[WikiDataClass1]

    // Lambda filter on a field (equivalent to wikiData.filter($"modified" > 1398823696))
    wikiDS.filter(x => x.modified > 1398823696).show(10)

    // map() returns a new Dataset; here a Dataset[(String, Int)]
    wikiDS
      .map(x => (x.title, if (x.text == null) 0 else x.text.length))
      .toDF("title", "text_length")
      .orderBy(desc("text_length"))
      .show(10, truncate = false)

    // Typed grouping: number of articles per user
    wikiDS
      .groupByKey(x => Option(x.username).getOrElse("anonymous"))
      .count()
      .orderBy(desc("count(1)"))
      .show(10)

    // Bring a few rows to the driver (take avoids collecting the whole dataset)
    wikiDS.take(2).foreach(println)

    ///////////////////////////////////
    // Exercises:
    // 1- Write a transformation withYear(df) that adds the year of 'modified'
    //    (hint: year(from_unixtime(...))) and count articles per year.
    // 2- Write a UDF that returns the longest word in 'title', then solve the
    //    same task with built-in functions only (split, explode, length).
    // 3- Using SQL, find the 5 users with the longest average text length.

    spark.stop()
  }
}

// Field names must match the column names in the Parquet file.
case class WikiDataClass1(id: Integer, title: String, modified: Long, text: String, username: String)
