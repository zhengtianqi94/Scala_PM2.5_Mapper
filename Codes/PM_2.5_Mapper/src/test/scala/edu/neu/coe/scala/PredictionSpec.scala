package edu.neu.coe.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by zheng on 2017/4/16.
  */
class PredictionSpec extends FlatSpec with Matchers{

  // Configuration of a new Spark config file and set the starting memory to be used
  val conf = new SparkConf().setAppName("PM 2.5 Mapper").setMaster("local[2]").set("spark.executor.memory", "512m");

  // Declare a new SparkContext
  val sc = new SparkContext(conf)

  // Declare a new sqlContext
  val sqlContext = new SQLContext(sc)

  var dir = new ListBuffer[String]()

  behavior of "Prediction with iteration time: 300, step size: 0.00000072, city selected to be 'San Antonio'"

  it should "Work for test source file of 2011" in {
    Try(new String("Test_2011.csv"))match {
      case Success(source) =>
        dir += source
        val prediction = Prediction.apply("San Antonio",dir,300,0.00000072,sqlContext)
        prediction.collect().toVector.length.shouldBe(57)
      case Failure(x) =>
        fail(x)
    }
  }

  it should "The regression model satisfies RMSE less than 20 with 1 test file" in {
    Try(new String("Test_2011.csv"))match {
      case Success(source) =>
        dir += source
        val prediction = Prediction.apply("San Antonio",dir,300,0.00000072,sqlContext)
        val result_test = {
          if (Prediction.getrootMeanSquaredError() < 20) true
          else false
        }
        result_test shouldBe true
      case Failure(x) =>
        fail(x)
    }
  }

  it should "The regression model satisfies RMSE less than 10 with 5 test file" in {
    Try(new String("Test_2011.csv")) match {
      case Success(source) =>
        dir += source
        dir += "Test_2012.csv"
        dir += "Test_2013.csv"
        dir += "Test_2014.csv"
        dir += "Test_2015.csv"
        val prediction = Prediction.apply("San Antonio", dir, 300, 0.00000072, sqlContext)
        val result_test = {
          if (Prediction.getrootMeanSquaredError() < 10) true
          else false
        }
        result_test shouldBe true
      case Failure(x) =>
        fail(x)
    }
  }
}
