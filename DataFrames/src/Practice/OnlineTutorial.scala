import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object OnlineTutorial extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "onlinetutorials")

  val viewsRdd = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/Assigniment/OnlineTurorial1")

  val chaptersRdd = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/Assigniment/OnlineTutorial3")

  val titilesRdd = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/Assigniment/OnlineTutorial2")

  val chaptersPercource = chaptersRdd.map(x => (x.split(",")(1).toInt, x.split(",")(0).toInt))

  val resChapters = chaptersPercource.map(x => (x._1, 1)).reduceByKey((x, y) => x + y).sortByKey().persist(StorageLevel.DISK_ONLY)

  
  
  resChapters.collect().foreach(println)
  
  println(resChapters.count)

}