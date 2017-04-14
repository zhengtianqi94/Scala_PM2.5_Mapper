package edu.neu.coe.scala

import java.io.File

import co.theasi.plotly._
import com.sun.jmx.snmp.Timestamp
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LassoWithSGD, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormat.{forPattern => formatFor}
import org.joda.time.{DateTime, Days}

import scala.collection.mutable.ListBuffer


case class Prediction(city: String, dir: ListBuffer[String], numIterations: Int, stepSize: Double)

object Prediction {
  def apply(city: String, dir: ListBuffer[String], numIterations: Int, stepSize: Double, sqlContext: SQLContext): RDD[Double] = {

    val filedir = dir.toList

    //The list of filedirs to train
    val dir_list = filedir.take(4)
    //The filedir to test
    val test_dir = filedir.apply(4)

    //Readin the first file to file the dataframe, incase Null pointer error
    var df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true")
      .load(filedir.apply(0))

    //Append other training files to dataframe
    for (x <- dir_list.drop(1)) {
      val df_temp = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true")
        .load(x)
      df = df union (df_temp)
    }

    //Readin test file
    var df_test = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true")
      .load(test_dir)

    //Date parse function
    val now = DateTime.now
    val beginDate = (new DateTime).withYear(2011)
      .withMonthOfYear(1)
      .withDayOfMonth(1)

    def daysTo(x: DateTime): Int = Days.daysBetween(beginDate, x).getDays + 1

    val format = new java.text.SimpleDateFormat("yyyy/MM/dd")

    def Datematch(date: Row): String = {
      if (date.get(0).isInstanceOf[Timestamp])
        format.format(date.get(0))
      else
        date.getString(0)
    }


    //Parse training data
    val train_city = df.where(df("CBSA Name") === city)
    val train_data_readin = train_city.select("Date Local", "Arithmetic Mean")
    //Error: Timestamp cannot be cast to string
    val pattern = "yyyy/MM/dd"
    val train_data_changeed: RDD[Row] = train_data_readin.rdd.map(row => Row(row(1), daysTo(DateTime.parse(Datematch(row), DateTimeFormat.forPattern(pattern)))))
    val train_data_prepared = train_data_changeed.map(x => x(0) + "," + x(1))
    //Generalize the data
    val train_data = train_data_prepared.map { line =>
      val parts = line.toString().split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).toDouble))
    }.cache()

    //Parse the test data
    val test_city = df_test.where(df_test("CBSA Name") === city)
    val test_data_readin = test_city.select("Date Local", "Arithmetic Mean")
    //Error: Timestamp cannot be cast to string
    val test_data_changeed: RDD[Row] = test_data_readin.rdd.map(row => Row(row(1), daysTo(DateTime.parse(Datematch(row), DateTimeFormat.forPattern(pattern)))))
    val test_data_prepared = test_data_changeed.map(x => x(0) + "," + x(1))
    //Generalize the data
    val test_data = test_data_prepared.map { line =>
      val parts = line.toString().split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).toDouble))
    }.cache()

    // Building the model
    val numIterations = 300
    val stepSize = 0.000000722
    val model = LassoWithSGD.train(train_data, numIterations, stepSize, 0.1)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = test_data.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val predictionvalues = test_data.map{ point =>
      val prediction = model.predict(point.features)
      prediction
    }

    //        predictionvalues.foreach(println)

    //Show predictions
    valuesAndPreds.collect().toVector.foreach(println)

    //Initialize the test metrics object
    val metrics = new RegressionMetrics(valuesAndPreds)

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")

    predictionvalues
  }
}