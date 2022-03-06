import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level


object SaveToJson extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "save to json")
  sparkConfig.set("spark.master", "local[3]")
  
  val spark = SparkSession.builder()
  .config(sparkConfig)
  .getOrCreate()
  
  

  
  val input = spark.read.csv("C:/Users/gotur/Documents/SparkDataSets/Assigniment/windowdata.csv").rdd
  
  
  
  
}