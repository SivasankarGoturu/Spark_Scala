import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object P_Aggrigates extends App{
  
    Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.app.name", "Different Ways To Solve The Problem")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()


  val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema" ,true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/order_data.csv")
    .load()
    
    // ====================== Simple Aggrigate ============================
    
    // -- Column Object
    
//    ordersDf.select(
//        
//        count("*").as("countOfRecords"),
//        sum("Quantity").as("sumOfQuantity"),
//        avg("UnitPrice").as("avgOfUnitPrice"),
//        countDistinct("InvoiceNo").as("DistinctInvoiceNumbers")
//        
//        
//    ).show
    
    // -- Coumn String
    
    ordersDf.selectExpr(
        
        "count(*) as countOfRecords",
        "sum(Quantity) as sumOfQuantity",
        "avg(UnitPrice) as avgOfUnitPrice",
        "count(Distinct InvoiceNo) DistinctInvoiceNumbers"
        
        
    ).show
    
    // -- Spark SQL
    
    
    //
    
//    ordersDf.show
    
    
    
    
}