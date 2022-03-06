import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import breeze.linalg.min
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object StationTemparature extends App{
  
  def parseLine(line: String) = {

    var station = line.split(",")(0)
    var temparature = line.split(",")(3).toFloat

    (station, temparature)

  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "mintemparature")

  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/Assigniment/StationTemp")

  val reqInput = input.map(parseLine)

  val minTemp = reqInput.reduceByKey((x, y) => min(x, y))

  minTemp.collect.foreach(println)
  
}