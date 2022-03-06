import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object SparkSessions extends App{
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "My spark session")
  sparkConf.set("spark.master", "local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  println("siva")
  
}