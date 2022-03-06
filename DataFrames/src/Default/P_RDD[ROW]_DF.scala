import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType


object P_RDD_DF extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val con = new SparkConf
  con.set("spark.app.name", "Conversions")
  con.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(con)
    .getOrCreate()

  val customerDF = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
    .load

  import spark.implicits._
  
  // This will gives you rdd[ROW]
  
  val customerRDD = customerDF.rdd
  
  val custSchema = StructType(List(
  
      StructField("customer_id", IntegerType),
      StructField("customer_fname", StringType),
      StructField("customerLname", StringType)
  
  ) 
      )
  
  val custDF = spark.createDataFrame(customerRDD, custSchema)
  
  custDF.show()
  
}