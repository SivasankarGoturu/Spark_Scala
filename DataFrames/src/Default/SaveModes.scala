import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object SaveModes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkcon = new SparkConf()
  sparkcon.set("spark.app.name", "save modes")
  sparkcon.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkcon)
    .getOrCreate()

    
    val ordersData = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("mode", "PERMISSIVE")
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/orders.csv")
    .load()
    
    
//    ordersData.write
//    .format("csv")
//    .mode(SaveMode.Overwrite)
//    .option("path","C:/Users/gotur/Documents/SavedDocuments")
//    .save()
//    
//    ordersData.write
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .option("path","C:/Users/gotur/Documents/SavedDocuments")
//    .save()
// 
    
    println(ordersData.rdd.getNumPartitions)
    
    val repartDf = ordersData.repartition(4)
    
    println(repartDf.rdd.getNumPartitions)
    
    ordersData.write
    .format("avro")
    .mode(SaveMode.Overwrite)
    .partitionBy("order_status")
//    .option("maxRecordsPerFile", 2000)
    .option("path","C:/Users/gotur/Documents/SavedDocuments")
    .save()
//    
}