import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object AvgOfFriendsConnection extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "Ago")
  def parseLine(lines: String) = {

    val columns = lines.split("::")
    val keys = columns(2).toInt
    val values = columns(3).toInt
    (keys, values)
  }

  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/FriendsData")

  val maping = input.map(parseLine)

  val mpVal = maping.mapValues(x => (x, 1))

  val output = mpVal.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

  val finelOut = output.map(x => (x._1, x._2._1 / x._2._2))

  finelOut.take(10).foreach(println)

}