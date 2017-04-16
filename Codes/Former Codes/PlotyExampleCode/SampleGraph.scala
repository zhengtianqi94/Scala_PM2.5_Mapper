import com.github.tototoshi.csv.CSVReader
import scala.io.Source
import com.github.tototoshi.csv._
import co.theasi.plotly._

/**
  * Created by zheng on 2017/3/17.
  */


object SampleGraph extends App {

  //this part is to give ploty your account information for API to check, the parameter should be Username, API Key
  implicit val server = new writer.Server {
    val credentials = writer.Credentials("zhengtianqi94", "Aj71bxZIQfDoJOLttgb0")
    val url = "https://api.plot.ly/v2/"
  }

  //Source data in csv format
  val dataURL = "https://raw.githubusercontent.com/plotly/datasets/master/api_docs/mt_bruno_elevation.csv"
  val reader = CSVReader.open(Source.fromURL(dataURL))

  val firstRow :: dataRows = reader.all

  val data = dataRows.map {
    case index :: values => values
    case _ => throw new IllegalStateException("Empty file")
  }

  val axisOptions = AxisOptions().noGrid.noLine.noZeroLine.noTickLabels.title("")

  //To draw a 3D graph, the three axis data in vector
  val p = ThreeDPlot()
    .withSurface(data, SurfaceOptions().colorscale("Earth").noScale)
    .xAxisOptions(axisOptions)
    .yAxisOptions(axisOptions)
    .zAxisOptions(axisOptions)

  //Call API, function draw
  val figure = Figure()
    .plot(p)
    .margins(0, 0, 0, 0)

  //the output file location, the parameter is filename which can be specified by user
  val outputFile = draw(figure, "custom-credentials", writer.FileOptions(overwrite=true))
  println(outputFile)
}
