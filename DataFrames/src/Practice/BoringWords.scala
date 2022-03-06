import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source

object BoringWords extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def boringSet = {

    val boringWords = Source.fromFile("C:/Users/gotur/Documents/SparkDataSets/BoringWords/Boringwords.txt").getLines()

    var boringSet: Set[String] = Set()

    for (i <- boringWords) {
      boringSet = boringSet + i
    }

    boringSet
    //    val boringWords = Source.fromFile("C:/Users/gotur/Documents/SparkDataSets/BoringWords/Boringwords.txt").getLines()
  }

  val sc = new SparkContext("local[*]", "alksdj")

  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/AddCompain")

  val boring = sc.broadcast(boringSet)

}