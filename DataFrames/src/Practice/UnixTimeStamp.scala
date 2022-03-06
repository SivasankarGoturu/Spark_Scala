package Practice

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UnixTimeStamp extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  val conf = new SparkConf()
  conf.set("spark.master", "local[2]")
  conf.set("spark.app.name", "unixtimestamps")
  
  val spark = SparkSession.builder()
  .config(conf)
  .getOrCreate()
  
  import spark.implicits._
  
  val mylist = List(
  ("2019-07-01 12:01:19","07-01-2019 12:01:19", "07-01-2019")
  )
  
 val myDf = mylist.toDF("time1", "time2", "time3")
  
 myDf.printSchema()
 myDf.show()
 
 
 val unixdate = myDf.select(
       unix_timestamp(col("time1")).as("unix_time1")
     , unix_timestamp(col("time2"),"MM-dd-yyyy HH:mm:ss").as("unix_time2")
     , unix_timestamp(col("time3"), "dd-MM-yyyy").as("unix_time3")
     , unix_timestamp().as("currentDate")
     
 )
 
 unixdate.show
 
 
 unixdate.select(
     from_unixtime(col("unix_time1")).as("normalTime1")
     ,from_unixtime(col("unix_time2")).as("normalTime2")
     ,from_unixtime(col("unix_time3"), "dd-MM-yy").as("normalTime3")
     ,from_unixtime(col("currentDate"))
).show
 
 
 
 
 
 
 
 
 
 
 
}