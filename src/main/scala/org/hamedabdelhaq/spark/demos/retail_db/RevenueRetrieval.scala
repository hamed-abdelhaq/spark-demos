package retail_db

import org.apache.spark.{SparkConf, SparkContext}
object RevenueRetrieval {

  def main(args: Array[String]): Unit = {

    // creating conf object holding spark context information required in the driver application
    val conf = new SparkConf().
      setMaster(args(0)).
      setAppName("Revenue Retrieval")

    // creating spark context
    val sc = new SparkContext(conf)

    // To avoid logging too many debugging messages
    sc.setLogLevel("ERROR")

    // using spark context to read a file from the file system and create an RDD accordingly
    // id, order_id, product_id, quantity, total_price, unit_price
    val orderItems = sc.textFile(args(1))
    println(orderItems.count());

    // doing a couple of transformation operations:
    // creating a new RDD 'revenuePerOrder' which is supposed to hold the accumulated revenue
    //  associated with each order.
    //  This is done first by selecting the second and fifth columns. Then, we group by key (order_id) using
    // reduceByKey. After that we generate a comma-separated records composing of 'order_id, revenue' to be
    // written to file
    val revenuePerOrder = orderItems
      .map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat))
      .reduceByKey((x,y) => (x + y))
      //.reduceByKey(_ + _)
      .map(oi => oi._1 + "," + oi._2)




    // This piece of scala code finds the average revenue for each order
    val average_by_key = orderItems
        .map(oi => (oi.split(",")(1).toInt, (oi.split(",")(4).toFloat, 1)))
        .reduceByKey((x,y) => ((x._1+y._1),(x._2+y._2)))
        .mapValues(x=>(x._1/ x._2))
        .map(oi => oi._1 + "," + oi._2)

    average_by_key.saveAsTextFile(args(2))

    //Practice-1.1) replace the above mapValues by map to achieve the same functionality.


    //Practice-1.2) Find the total number of items sold for each order






    //Practice-1.3) Find the average number of items sold for each product





  }

}
