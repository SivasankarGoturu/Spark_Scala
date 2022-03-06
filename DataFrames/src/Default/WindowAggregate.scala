import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object WindowAggregate extends App{
  
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
    .option("inferSchema", true)
    .option("header", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/windowdata.csv")
    .load()
    
    
    
    val newDf = ordersDf.toDF("Country", "WeekId", "numInvoices", "totalQuantity", "invoiceValue")
    
    val myWin = Window.partitionBy("Country")
    .orderBy("WeekId")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    val resDf = newDf.withColumn("runningSum", sum("invoiceValue").over(myWin))
    
    val ne = resDf.rdd
    
    
    resDf.show
    
  spark.stop()
}