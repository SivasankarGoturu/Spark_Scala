import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object MyPractice extends App{


  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.app.name", "MyApplication")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val custSchema = StructType(List(

    StructField("custId", IntegerType),
    StructField("fname", StringType),
    StructField("lname", StringType),
    StructField("password", StringType),
    StructField("street", StringType),
    StructField("city", StringType),
    StructField("state", StringType),
    StructField("zipcode", StringType)))

  val customerDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .schema(custSchema)
    .option("header", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
    .load

  import spark.implicits._

  case class customers(custId :Int, fname:String, lname:String, password: String, street: String, city: String, state: String, zipcode: String)
  
  //customer_id: String, customer_fname: String, customer_lname: String, customer_email: String, customer_password: String, customer_street: String, customer_city: String, customer_state: String, customer_zipcode: String
  
  val custds = customerDf.as[customers]

  custds.show
  
  
  custds.write
  .format("orc")
  .mode(SaveMode.Append)
  .partitionBy("zipcode")
  .option("path", "C:/Users/gotur/Documents/SparkDataSets/saveCodes")
  .save()

  

}