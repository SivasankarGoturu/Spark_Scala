import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Dag extends App{
  
//  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "My spark App")
  sparkConfig.set("spark.master", "local[4]")

  val spark = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

  val customersDf = spark.read
    .format("json")
    .option("mode", "PERMISSIVE")
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customersCsv/")
    .load

  val ordersDf = spark.read
    .format("json")
    .option("mode", "PERMISSIVE")
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/ordersCsv/")
    .load
  
//    spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
    
    val joinCondition = customersDf.col("customer_id") === ordersDf.col("order_customer_id")
    
    val join = "right"
    
    ordersDf.join(customersDf,joinCondition, join).show
    
    scala.io.StdIn.readLine()
    
    spark.stop
    
//    customersDf.show
//    
//    ordersDf.show

}