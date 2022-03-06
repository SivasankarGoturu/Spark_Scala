import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object AddCampain extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseInt(line: String) = {
    val fileds = line.split(",")

    (fileds(10).toFloat, fileds(0))

  }
  val sc = new SparkContext("local[*]", "add compain")

  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/AddCompain")

  val flat = input.map(parseInt)

  val fmap = flat.flatMapValues(x => x.split(" ")).map(x => (x._2.toLowerCase(),x._1)).reduceByKey((x,y) => x+y)
  
  val output = fmap.sortBy(x => x._2,false)

  output.take(10).foreach(println)

}