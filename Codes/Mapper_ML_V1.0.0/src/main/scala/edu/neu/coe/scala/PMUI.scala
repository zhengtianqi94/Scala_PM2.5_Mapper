package edu.neu.coe.scala

// import all packages under scala.swing
import java.awt.Dimension
import java.io.File

import co.theasi.plotly._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormat.{forPattern => formatFor}
import org.joda.time.{DateTime, Days}

import scala.collection.mutable.ListBuffer
import scala.swing._
import scala.swing.event.ButtonClicked

object PMUI extends SimpleSwingApplication {

  // File direction from the user input filed, use as a variable
  // Path variable
  var dir = new ListBuffer[String]()

  var cityList = new ListBuffer[String]()

  val Submit = new Button {
    text = "Submit"
  }

  val Draw = new Button {
    text = "Draw"
  }

  val label1 = new Label {
    text = "Ploty Username"
  }

  val textf1 = new TextField() {
    maximumSize = new Dimension(200, 30)
  }

  val label2 = new Label {
    text = "Ploty API Key"
  }

  val textf2 = new TextField() {
    maximumSize = new Dimension(200, 30)
  }

  val textf3 = new TextField() {
    maximumSize = new Dimension(200, 30)
  }

  val panel1 = new GridPanel(1, 2) {
    contents += label1
    contents += textf1
    maximumSize = new Dimension(300, 30)
  }

  val panel2 = new GridPanel(1, 2) {
    contents += label2
    contents += textf2
    maximumSize = new Dimension(300, 30)
  }

  val paneldropdown = new GridPanel(1, 2) {
    val city = new ComboBox(cityList)
    contents += city
  }

  var fileChooser = new FileChooser(new File("."))

  fileChooser.title = "fileChooser"

  val Choose1 = new Button {
    text = "Please choose source data file";
  }

  val Choose2 = new Button {
    text = "Please choose source data file"
  }

  val Choose3 = new Button {
    text = "Please choose source data file"
  }

  val Choose4 = new Button {
    text = "Please choose source data file"
  }

  val Choose5 = new Button {
    text = "Please choose source data file"
  }


  val panel5 = new FlowPanel {
    contents += Choose1
    maximumSize = new Dimension(300, 30)
  }
  val panel6 = new FlowPanel {
    contents += Choose2
    maximumSize = new Dimension(300, 30)
  }
  val panel7 = new FlowPanel {
    contents += Choose3
    maximumSize = new Dimension(300, 30)
  }
  val panel8 = new FlowPanel {
    contents += Choose4
    maximumSize = new Dimension(300, 30)
  }
  val panel9 = new FlowPanel {
    contents += Choose5
    maximumSize = new Dimension(300, 30)
  }

  // This is the main function, which replaces the main(Args[String]) function with MainFrame
  def top = new MainFrame {

    title = "PM2.5 Mapper"

    // Configuration of a new Spark config file and set the starting memory to be used
    val conf = new SparkConf().setAppName("PM2.5 Mapper").setMaster("local[1]").set("spark.executor.memory", "512m");

    // Declare a new SparkContext
    val sc = new SparkContext(conf)

    // Declare a new sqlContext
    val sqlContext = new SQLContext(sc)

    contents = new BoxPanel(Orientation.Vertical) {
      contents += panel1
      contents += panel2
      contents += paneldropdown
      contents += panel5
      contents += panel6
      contents += panel7
      contents += panel8
      contents += panel9
      contents += Submit
      contents += Draw
      border = Swing.EmptyBorder(20, 20, 20, 20)
    }

    size = new Dimension(400, 400)

    listenTo(Submit)
    listenTo(Draw)
    listenTo(Choose1)
    listenTo(Choose2)
    listenTo(Choose3)
    listenTo(Choose4)
    listenTo(Choose5)

    def getCity(path: String) = {
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .load(path)
      val cities = df.select("CBSA Name")

      //Error: Some CBSA name is null, make a function to get rid of null pointers
      cities.rdd.distinct().foreach(row => cityList += row(0).toString)
      //      cityList.foreach(println)
      repaint()
    }

    reactions += {
      case ButtonClicked(Choose1) => {
        val result = fileChooser.showOpenDialog(panel5);
        if (result == FileChooser.Result.Approve) {
          Choose1.text = fileChooser.selectedFile.getPath;
          dir.+=(fileChooser.selectedFile.getPath);
        }
        getCity(fileChooser.selectedFile.getPath)
      }
      case ButtonClicked(Choose2) => {
        val result = fileChooser.showOpenDialog(panel6);
        if (result == FileChooser.Result.Approve) {
          Choose2.text = fileChooser.selectedFile.getPath;
          dir.+=(fileChooser.selectedFile.getPath);
        }
      }
      case ButtonClicked(Choose3) => {
        val result = fileChooser.showOpenDialog(panel7);
        if (result == FileChooser.Result.Approve) {
          Choose3.text = fileChooser.selectedFile.getPath;
          dir.+=(fileChooser.selectedFile.getPath);
        }
      }
      case ButtonClicked(Choose4) => {
        val result = fileChooser.showOpenDialog(panel8);
        if (result == FileChooser.Result.Approve) {
          Choose4.text = fileChooser.selectedFile.getPath;
          dir.+=(fileChooser.selectedFile.getPath);
        }
      }
      case ButtonClicked(Choose5) => {
        val result = fileChooser.showOpenDialog(panel9);
        if (result == FileChooser.Result.Approve) {
          Choose5.text = fileChooser.selectedFile.getPath;
          dir.+=(fileChooser.selectedFile.getPath);
        }
      }

      case ButtonClicked(Submit) => {
        // Declare the operation of the sqlContext which here is read

        val city = paneldropdown.city.item

        val filedir = dir.toList

        var df = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "true")
          .load(filedir.apply(0))
        for (x <- filedir.drop(1)) {
          val df_temp = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true")
            .load(x)
          df = df union(df_temp)
        }
//        df.show()



        val now = DateTime.now
        val beginDate = (new DateTime).withYear(2011)
          .withMonthOfYear(1)
          .withDayOfMonth(1)

        def daysTo(x: DateTime): Int = Days.daysBetween(beginDate, x).getDays + 1

        //TODO form a RDD with label Arithmetic Mean, and characters: Latitude, Longitude, Date Local
        val selectedCity = df.where(df("CBSA Name") === city)
        println(city)

        val selectedData = selectedCity.select("Date Local", "Arithmetic Mean")

        //Error: Timestamp cannot be cast to string
        val pattern = "yyyy/MM/dd"
        val changedData: RDD[Row] = selectedData.rdd.map(row => Row(row(1), daysTo(DateTime.parse(row.getString(0), DateTimeFormat.forPattern(pattern)))))
        //        changedData.collect.foreach(println)
        val preparedData = changedData.map(x => x(0) + "," + x(1))

        //  val rows: RDD[Row] = selectedData.rdd
        //TODO this one should be implemented, and after that this RDD can be directly used as training data for macihne learning

        // parse the data

        val parsedData = preparedData.map { line =>
          val parts = line.toString().split(',')
          LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).toDouble))
        }.cache()

        // Building the model
        val numIterations = 300
        val stepSize = 0.00000001
        val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

        val dateD = selectedCity.select("Date Local")
        val days: RDD[Row] = dateD.rdd.map(row => Row(daysTo(DateTime.parse(row.getString(0), DateTimeFormat.forPattern(pattern)))))
        //        val Day_list = days.map(r => r(0).asInstanceOf[Double]).collect().toVector

        //Convert RDD[Row] to RDD[Vector]
        val prediction = model.predict(days.map { row => Vectors.dense(row.getAs[Integer](0).toDouble)})
        prediction.take(20).foreach(println)

        // Evaluate model on training examples and compute training error
        val valuesAndPreds = parsedData.map { point =>
          val prediction = model.predict(point.features)
          (point.label, prediction)
        }
        val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
        println("training Mean Squared Error = " + MSE)

      }

      case ButtonClicked(Draw) => {

        val city = paneldropdown.city.item
        println(city)
        //get the days
        val now = DateTime.now
        val beginDate = (new DateTime).withYear(2011)
          .withMonthOfYear(1)
          .withDayOfMonth(1)

        def daysTo(x: DateTime): Double = Days.daysBetween(beginDate, x).getDays + 1

        implicit val server = new writer.Server {
          val credentials = writer.Credentials("zhengtqwanglx", "mwApPLKhhP5okOoQDGNb")
          val url = "https://api.plot.ly/v2/"
        }

        // Create a sparksession for further use
        val spark = SparkSession
          .builder()
          .appName("Spark SQL basic example")
          .config("spark.some.config.option", "some-value")
          .getOrCreate()

        // Declare the operation of the sqlContext which here is read
        val filedir = dir.toList

        var df = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "true")
          .load(filedir.apply(0))
        for (x <- filedir.drop(1)) {
          val df_temp = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true")
            .load(x)
          df = df union(df_temp)
        }

        //        Latitude
        //        Longitude
        //        Date Local
        //        Arithmetic Mean - RDD label
        //        State Name
        //        City Name

        // Select required fields from source datafile
        val selectedCity = df.where(df("CBSA Name") === city)
        //days and arithmeticMean
        val pattern = "yyyy/MM/dd"
        val dateD = selectedCity.select("Date Local")
        val arithmeticMean = selectedCity.select("Arithmetic Mean")
        val days: RDD[Row] = dateD.rdd.map(row => Row(daysTo(DateTime.parse(row.getString(0), DateTimeFormat.forPattern(pattern)))))
        days.collect.foreach(println)
        arithmeticMean.rdd.collect.foreach(println)

        //        val draw_with_data = df.select("Date Local", "Arithmetic Mean")
        //
        //        val Day = draw_with_data.select("Date Local")
        //        val Concentration = draw_with_data.select("Arithmetic Mean")

        val Day_list = days.map(r => r(0).asInstanceOf[Double]).collect().toVector
        val Concentration_list = arithmeticMean.rdd.map(r => r(0).asInstanceOf[Double]).collect().toVector

        val p = Plot().withScatter(Day_list, Concentration_list)

        val figure = Figure()
          .plot(p)
          .margins(0, 0, 0, 0)

        //the output file location, the parameter is filename which can be specified by user
        val outputFile = draw(figure, "custom-credentials", writer.FileOptions(overwrite = true))

        println("Finished!")

        //TODO here we need to form a RDD with label Arithmetic Mean, and characters: Latitude, Longitude, Date Local
        //TODO this one should be implemented, and after that this RDD can be directly used as training data for macihne learning
        //Test

      }
    }
  }
}












