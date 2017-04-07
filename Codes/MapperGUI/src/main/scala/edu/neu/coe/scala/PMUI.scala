package edu.neu.coe.scala

// import all packages under scala.swing
import java.awt.Dimension
import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer
import scala.swing._
import scala.swing.event.ButtonClicked

object PMUI extends SimpleSwingApplication {

  // File direction from the user input filed, use as a variable
  // Path variable
  var dir = new ListBuffer[String]()

  val button = new Button {
    text = "Submit"
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
    val conf = new SparkConf().setAppName("PM2.5 Mapper").setMaster("local[2]").set("spark.executor.memory", "512m");

    // Declare a new SparkContext
    val sc = new SparkContext(conf)

    // Declare a new sqlContext
    val sqlContext = new SQLContext(sc)

    contents = new BoxPanel(Orientation.Vertical) {
      contents += panel1
      contents += panel2
      contents += panel5
      contents += panel6
      contents += panel7
      contents += panel8
      contents += panel9
      contents += button
      border = Swing.EmptyBorder(20, 20, 20, 20)
    }

    size = new Dimension(400, 400)

    listenTo(button)
    listenTo(Choose1)
    listenTo(Choose2)
    listenTo(Choose3)
    listenTo(Choose4)
    listenTo(Choose5)

    reactions += {
      case ButtonClicked(Choose1) => {
        val result = fileChooser.showOpenDialog(panel5);
        if (result == FileChooser.Result.Approve) {
          Choose1.text = fileChooser.selectedFile.getPath;
          dir.+=(fileChooser.selectedFile.getPath);
        }
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
      case ButtonClicked(button) => {

        // Declare the operation of the sqlContext which here is read
        val df = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "true") // Automatically infer data types
          .load(dir.toList.mkString(","))

        //        Latitude
        //        Longitude
        //        Date Local
        //        Arithmetic Mean - RDD label
        //        State Name
        //        City Name

        // Select required fields from source datafile
        val selectedData = df.select("Latitude", "Longitude", "Date Local", "Arithmetic Mean", "State Name", "City Name")

        // Generate the dataframe to RDD structure for further use
        val rows: RDD[Row] = selectedData.rdd

        //TODO here we need to form a RDD with label Arithmetic Mean, and characters: Latitude, Longitude, Date Local
        //TODO this one should be implemented, and after that this RDD can be directly used as training data for macihne learning
        //Test

      }
    }
  }
}












