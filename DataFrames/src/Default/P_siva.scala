import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.log4j.Level

object P_siva extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "appname")

  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")

  val reqFields = input.map(x => x.split(",")(1))

  val words = reqFields.flatMap(x => x.split(","))

  val mapinputs = words.map(x => (x, 1))

  mapinputs.toDebugString
  
  val output = mapinputs.reduceByKey((x, y) => x + y).sortBy(x => x._2)

  val foutput = output.collect()

  foutput.foreach(println)

}