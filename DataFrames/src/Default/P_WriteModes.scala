import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


object Practice extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spConf = new SparkConf()
  spConf.set("spark.app.name", "men")
  spConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(spConf)
    .enableHiveSupport()
    .getOrCreate()
    
    val custDf = spark.read
    .format("json")
    .option("mode", "FAILFAST")
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customers.json")
    .load
    
    /*
    // SaveDataIn File
    custDf.write
    .format("csv")
    .mode(SaveMode.Append)
    .partitionBy("customer_state","customer_city")
    .option("maxRecordsPerFile", 100)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customerdata")
    .save()
    */
    
    val resdf = custDf.where("customer_state in ('TX', 'CA', 'PR')")
    
    spark.sql("create database if not exists customer")
    
//    resdf.write
//    .format("csv")
//    .mode(SaveMode.Append)
//    .partitionBy("customer_state" ,"customer_city")
//    .bucketBy(4, "customer_id")
//    .option("maxRecordsPerFile", 3)
//    .saveAsTable("customer.customers")
    
    spark.catalog.listTables("customer").show
    
//    custDf.show
    
}