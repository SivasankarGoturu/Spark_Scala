import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object ModesOfReadingFile extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkCon = new SparkConf()
  sparkCon.set("spark.app.name", "Modes of reading")
  sparkCon.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkCon)
  .getOrCreate()
  
  // CSV
  
 /* val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "C:/Users/gotur/Documents/SparkDataSets/DataSets/orders.csv")
  .load */
  

  
  
  // Json
  
  val ordersDf = spark.read
  .format("json")
  .option("path", "C:/Users/gotur/Documents/SparkDataSets/DataSets/Players.json")
  .option("mode", "PERMISSIVE")  // It will display corrupted records
//  .option("mode", "DROPMALFORMED") // It will skip the records
//  .option("mode", "FAILFAST")  // It will through an error
  .load  
  
  // Parquet  
  
//  val ordersDf = spark.read
//  .option("path", "C:/Users/gotur/Documents/SparkDataSets/DataSets/users.parquet")
//  .load
   
  import spark.implicits._
  
  ordersDf.show(false)

//  ordersDf.withColumn("name", concat($"age",' ',$"player_id")).show

  
  
  ordersDf.printSchema()
  
  
  
  
  
}