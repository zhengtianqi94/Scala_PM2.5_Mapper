package edu.neu.coe.scala

// import all packages under scala.swing
import java.awt.Dimension
import java.io.File

import co.theasi.plotly._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
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
    val conf = new SparkConf().setAppName("PM2.5 Mapper").setMaster("local[2]").set("spark.executor.memory", "512m");

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

    //Get cities from the first file, filter out cities with name "not in a city"
    def getCity(path: String) = {
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .load(path)
      val cities = df.select("City Name")

      cities.filter(!(cities("City Name") === "Not in a city")).rdd.distinct().foreach(row => cityList += row(0).toString)
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

      case ButtonClicked(Draw) => {

        val city = paneldropdown.city.item

        val prediction = Prediction.apply(city, dir, 300, 0.000000722, sqlContext)

        //get the days
        val now = DateTime.now

        def beginDate(y: String) = (new DateTime).withYear(y.toInt)
          .withMonthOfYear(1)
          .withDayOfMonth(1)

        def daysTo(x: DateTime, y: String): Double = Days.daysBetween(beginDate(y), x).getDays + 1

        implicit val server = new writer.Server {
          val credentials = writer.Credentials("zhengtqwanglx", "VYBwvhFPbylxxEVUuO86")
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

        val df: DataFrame = sqlContext.read
          .format("com.databricks.spark.csv")
          // Use first line of all files as header
          .option("header", "true")
          .option("inferSchema", "true")
          .load(filedir.apply(0))
        var dfSeq: Seq[(DataFrame)] = Seq(df)
        for (x <- filedir.drop(1)) {
          val df_temp = sqlContext.read
            .format("com.databricks.spark.csv")
            // Use first line of all files as header
            .option("header", "true")
            .option("inferSchema", "true")
            .load(x)
          dfSeq = dfSeq :+ df_temp
        }

        //Get all data of user choosen city
        val selectedCity = dfSeq.map(x =>
          x.where(x("City Name") === city)
        )

        //Get data needed
        val selectedData = selectedCity.map(x =>
          x.select("Date Local", "Arithmetic Mean")
        )

        //Format the date to String
        val format = new java.text.SimpleDateFormat("yyyy/MM/dd")

        def Datematch(date: Row): String = {
          if (date.get(0).isInstanceOf[Timestamp])
            format.format(date.get(0))
          else
            date.getString(0)
        }

        //Parse year
        val selectedYear = selectedData.map(x =>
          x.select("Date Local").rdd.map(row => Row(Datematch(row).substring(0, 4))).distinct()
        )
        selectedYear.foreach(println)
        var year: String = selectedYear.apply(0).first().getString(0)
        var years: ListBuffer[String] = ListBuffer(year)
        for (x <- selectedYear.drop(1)) {
          years = years :+ x.first().getString(0)
        }

        //Parse date
        val pattern = "yyyy/MM/dd"

        def parseData(x: DataFrame, y: String): RDD[Row] = x.rdd.map(row => Row(daysTo(DateTime.parse(Datematch(row), DateTimeFormat.forPattern(pattern)), y), row(1)))

        val firstParse: RDD[Row] = parseData(selectedData.apply(0), years.apply(0));
        var parsedData: Seq[RDD[Row]] = Seq(firstParse)

        if (selectedData.size > 1) {
          for (a <- 0 to selectedData.size - 2) {
            parsedData = parsedData :+ parseData(selectedData.drop(1).apply(a), years.drop(1).apply(a))
          }
        }

        val days = parsedData.map(x =>
          x.map(row => Row(row(0)))
        )
        val arithmeticMean = parsedData.map(x =>
          x.map(row => Row(row(1)))
        )

        val Day_list = days.map(x =>
          x.map(r => r(0).asInstanceOf[Double]).collect().toVector
        )
        val Concentration_list = arithmeticMean.map(x =>
          x.map(r => r(0).asInstanceOf[Double]).collect().toVector
        )
        val predicted = prediction.collect().toVector

        // Options common to both traces
        val commonOptions = ScatterOptions()
          .mode(ScatterMode.Marker)
          .name("Actual PM 2.5 Concentration in " + city)
          .marker(MarkerOptions().size(12).lineWidth(1))

        // Options common to both axis
        val commonAxisOptions = AxisOptions()
          .withTickLabels

        //Options common to legend
        val commonLegendOptions = LegendOptions()
          .x(1.02)
          .y(1)
          .fontSize(12)
          .xAnchor(XAnchor.Left)
          .yAnchor(YAnchor.Top)

        //Options to each axis
        val xAxisOptions = commonAxisOptions.title("Day")
        val yAxisOptions = commonAxisOptions.title("PM 2.5 Concentration")

        //Draw graph by year
        for (a <- 0 to Day_list.length - 2) {

          //Define plot options
          val p = Plot().withScatter(Day_list.apply(a), Concentration_list.apply(a), commonOptions)
            .xAxisOptions(xAxisOptions)
            .yAxisOptions(yAxisOptions)

          /*
          Define figure options, with a API error of legend, there is something wrong for legned API that cannot set "ShowLegend" option to true
          so that legend cannot be displayed
          */
          val figure = Figure()
            .legend(commonLegendOptions)
            .plot(p)
            .title("PM 2.5 of " + years.apply(a) + " in " + city)

          //the output file is stored in user's file directory with serialized names
          val outputFile = draw(figure, "PM 2.5 of " + years.apply(a) + " in " + city)

          println("Draw graph for year " + years.apply(a) + " Finished")
        }

        //Define plot options of last year, together with predicted line
        val p = Plot().withScatter(Day_list.apply(Day_list.length - 1), Concentration_list.apply(Day_list.length - 1), commonOptions)
          .withScatter(Day_list.apply(Day_list.length - 1), predicted, ScatterOptions()
            .mode(ScatterMode.Line)
            .name("Prediction Line")
            .marker(MarkerOptions().size(12).lineWidth(1)))
          .xAxisOptions(xAxisOptions)
          .yAxisOptions(yAxisOptions)

        /*
        Define figure options, with a API error of legend, there is something wrong for legned API that cannot set "ShowLegend" option to true
        so that legend cannot be displayed
        */
        val figure = Figure()
          .legend(commonLegendOptions)
          .plot(p)
          .title("PM 2.5 of " + years.apply(Day_list.length - 1) + " in " + city)

        //the output file is stored in user's file directory with serialized names
        val outputFile = draw(figure, "PM 2.5 of " + years.apply(Day_list.length - 1) + " in " + city)

        println("Draw graph for year " + years.apply(Day_list.length - 1) + " Finished")

      }
    }
  }
}












