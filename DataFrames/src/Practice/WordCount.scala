import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Word Count App")

  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/der")

  val words = input.flatMap(x => x.split(" "))
  
  val mapingWords = words.map(x => (x,1))
  
  val countingWords = mapingWords.reduceByKey((x,y) => x+y).sortBy(x => x._2)
  
  countingWords.collect().foreach(println)
  
  scala.io.StdIn.readLine()

}