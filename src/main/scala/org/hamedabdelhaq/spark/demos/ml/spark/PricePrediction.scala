package ml.spark

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor}
import org.apache.spark.sql.SparkSession

object PricePrediction {

  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)


    // create a SparkSession
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Regression basic example")
      .getOrCreate()
    // load data
    val data = spark.read.format("csv").
      option("header", "true").
      option("inferSchema", "true" ).
      load("data/boston_housing.csv")

    //create features vector
    // Excluding the label column (medv)
    val feature_columns = data.columns.slice(0,data.columns.length-1)

    //printing the feature columns to make sure we are targeting the right columns
    feature_columns.foreach(println);


    // assembling the features as one feature column called "features",
    // Note: assembler is a transformer
    val assembler = new VectorAssembler().
      setInputCols(feature_columns).
      setOutputCol("features")

    //combining the "features" column with the original dataset columns including 'medv'
    val data_2 = assembler.transform(data)


    //have a look at the main schema of the transformed dataset
    data_2.printSchema();
    //sys.exit(0);

    // train/test split

    // splitting dataset into trainig and testing datasets
    val splits = data_2.randomSplit(Array(0.7, 0.3), seed = 11L)
    val train = splits(0)//.cache()
    val test = splits(1)



    // Random Forest Regressor
//    val linearRegressor = new LinearRegression().
//      setFeaturesCol("features").
//      setLabelCol("medv")

    // train the model
//    val model = linearRegressor.fit(train)
    //evaluation
//    val evaluation_summary = model.evaluate(test)
//    println(evaluation_summary.meanAbsoluteError)
//    println(evaluation_summary.rootMeanSquaredError)
//    println(evaluation_summary.r2)
//    // predicting values
//    val predictions = model.transform(test)
//    predictions.show()//.select(predictions.columns[13:]).show()
//    //here I am filtering out some columns just for the figure to fit



    val randomForestRegressor = new RandomForestRegressor().
      setFeaturesCol("features").
      setLabelCol("medv");

    val model = randomForestRegressor.fit(data_2)
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("medv")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val r2 = evaluator.evaluate(predictions)
    println("(R2) on test data = " + r2)

    predictions.show()
  }

}
