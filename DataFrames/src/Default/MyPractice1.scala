import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode


object MyPractice1 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.app.name", "Different Ways To Solve The Problem")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()
    
      val customerDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("inferSchema", true)
    .option("header", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
    .load
    
    val df2 = customerDf.select("customer_id", "customer_fname","customer_zipcode")

    spark.sql("create database if not exists mydb")
    spark.sql("create table if not exists mytable")
    
    df2.write
    .format("parquet")
    .mode(SaveMode.Append)
    .bucketBy(4, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("mydb.mytable.aggCusts")
  
  
}