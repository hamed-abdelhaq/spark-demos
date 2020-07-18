package ml.spark

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession;

object TitanicSurvivalPrediction {

  def main(args: Array[String]): Unit = {
    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR);

    val spark = SparkSession.builder().master("local").getOrCreate()

    // For more information about the data:
    // https://www.kaggle.com/c/titanic/data
    val data = spark.read.
      option("header", "true").
      option("inferSchema","true").
      format("csv").
      load("data/titanic-dataset/train.csv")
    data.show(10);

    data.printSchema();

    import spark.implicits._


    val datapreparedAll = data.
      select(data("Survived").as("label"),
        'Pclass,
        'Name,
        'Sex,
        'Age,
        'SibSp,
        'Parch,
        'Fare,
        'Embarked)

    println(s"Number of samples before removing records with Nulls: ${datapreparedAll.count()}");

    val dataprepared = datapreparedAll.na.drop();
    println(s"Number of samples after removing records with Nulls: ${dataprepared.count()}");



    // Converting Strings into numerical values
    val genderIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex");
    val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkedIndex");

    // Converting numerical values into one hot encoding 0,1
    val genderEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVec");
    val embarkedEncoder = new OneHotEncoder().setInputCol("EmbarkedIndex").setOutputCol("EmbarkedVec");


    // (Label, features)
    val assembler = new VectorAssembler().
      setInputCols(Array("Pclass", "SexVec","Age", "SibSp", "Parch","Fare","EmbarkedVec")).
      setOutputCol("features")

    val Array(training, test) = dataprepared.randomSplit(Array(0.7,0.3), seed = 12345);

    // set up the pipeline

    val lr = new LogisticRegression();

    val pipeline  = new Pipeline().
      setStages(
        Array(genderIndexer,
          embarkIndexer,
          genderEncoder,
          embarkedEncoder,
          assembler,
          lr));

    val model = pipeline.fit(dataprepared);



    val result = model.transform(test);

    result.show(100)
    //System.exit(0);
    //val predictionAndLabels = result.select($"prediction", $"label").as[(Double, Double)].rdd;



    // Practices:
    //1) rewrite the above code using pyspark
    //2) evaluate the model built above by calculating:
    //  1- the accuracy, precision, and recall
    //  2- AUC (extra)
    //3) Use Random forest and compare its results with Logistic regression
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val f_score = evaluator.evaluate(result)
    println(s"Test set f-score = $f_score")






  }

}
