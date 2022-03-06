import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import scala.reflect.api.materializeTypeTag

object ScenarioBasedQuestion extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "My spark App")
  sparkConfig.set("spark.master", "local[4]")

  val spark = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

  val myList = List(
    (1, "2013-07-25 00:00:00.0", 11599, "CLOSED"), 
    (2, "2013-07-25 00:00:00.0", 11599, "PENDING_PAYMENT"), 
    (3, "2013-07-25 00:00:00.0", 12111, "COMPLETE"), 
    (4, "2013-07-25 00:00:00.0", 8827, "CLOSED"), 
    (5, "2013-07-25 00:00:00.0", 11318, "COMPLETE"))

    
  
    
  val myDf = spark.createDataFrame(myList).toDF("orderId", "orderDate", "customerId", "orderStatus")
  
  val resuldDf = myDf.withColumn("orderDate", unix_timestamp(col("orderDate").cast(DateType)))
  .dropDuplicates("orderDate","customerId")
  .withColumn("Uid", monotonically_increasing_id)
  .drop("orderId")
  .sort("orderDate")
  
  resuldDf.show
  resuldDf.printSchema()  

}