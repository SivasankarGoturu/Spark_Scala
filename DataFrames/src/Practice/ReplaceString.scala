package Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object ReplaceString extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "Clean Json Data")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val myList = Seq(
    (1, "14851 Jeffrey Rd", "DE"),
    (2, "43421 Margarita St", "NY"),
    (3, "13111 Siemon Ave", "CA"))
    
    import spark.implicits._
    
    val myDf = spark.createDataFrame(myList).toDF("id","address", "state")
    
    val df1 = myDf.withColumn("address", regexp_replace($"address","Rd","siva"))
    
    df1.withColumn("mytime", current_timestamp()).withColumn("mytime", date_format($"mytime", "yyyy/MM/dd").as("formattedData")).show(false)
    

    
//    for(i<-myList) println(i)
    
    
    
}