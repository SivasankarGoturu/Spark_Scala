import org.apache.spark.SparkContext

object AgeAllow extends App {

  def parseLine(Line: String) = {

    val fields = Line.split(",")

    (fields(0), fields(1).toInt, fields(2), if (fields(1).toInt > 18) "Y" else "N")
  }

  val sc = new SparkContext("local[*]", "Ago")

  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/Assigniment/Age")

  val reqInput = input.map(parseLine)

  reqInput.take(10).foreach(println)
}