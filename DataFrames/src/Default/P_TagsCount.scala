import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Unst extends App {

Logger.getLogger("org").setLevel(Level.ERROR)

  val con = new SparkConf
  con.set("spark.master", "local[2]")
  con.set("spark.app.name", "sklj")

  val spark = SparkSession.builder()
    .config(con)
    .getOrCreate()

  val myList = List(
    (1, "This is my tweet #abc #abc#abc2#abc3 #bcd"))

  val myDf = spark.createDataFrame(myList).toDF("tweetId", "tweet")

  val myRdd = myDf.rdd

  val rddString = myRdd.map(_.mkString(","))

  val resRdd = rddString.map(x => x.split(",")(1)).flatMap(x => x.split(" "))
  
  val filterRdd = resRdd.filter(x => x.contains("#")).flatMap(x => x.split("#"))
 
  val filterRdd2 = filterRdd.filter(x => !(x.isEmpty()))

  val resultRdd = filterRdd2.map(x => (x,1)).reduceByKey((x,y) => x+y).sortBy(x => x._2)
  
  resultRdd.collect.foreach(println)

}