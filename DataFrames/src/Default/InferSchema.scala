import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InferSchema extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "My spark App")
  sparkConfig.set("spark.master", "local[4]")

  val spark = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

  // Programatic Approach of infering schema

  //  val ordersSchema = StructType(List(
  //
  //      StructField("orderid" ,IntegerType, true),
  //      StructField("orderDate" ,TimestampType),
  //      StructField("customerId" ,IntegerType),
  //      StructField("status" ,StringType)
  //
  //  ))

  // Using DDL String

  val ordersSchema = "orderid Int, orderDate Date, customerID Int, status String"

  
  val ordersData = spark.read
    .format("csv")
    .schema(ordersSchema)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/DataSets/orders.csv")
    .option("header", true)
    .load

 
    
  ordersData.show
  ordersData.printSchema()

}