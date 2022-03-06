package Practice

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel

object OOM extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "myapp")
  
  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/Week13/bigLogNewtxt/bigLogNew.txt")
  
  val splitting = input.map(x => x.split(": ")(0))

  val maping = splitting.map(x => (x,1)).persist(StorageLevel.MEMORY_AND_DISK)
    
  maping.collect.foreach(println)
  
//  val redu = maping.reduceByKey((x,y) => x+y)
  
//  redu.collect.foreach(println)
  
//  scala.io.StdIn.readLine()
  
}