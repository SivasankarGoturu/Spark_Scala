import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.functions._

object Practice2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "My spark App")
  sparkConfig.set("spark.master", "local[4]")

  val spark = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

//    "Country", "WeekId", "numInvoices", "totalQuantity", "invoiceValue"
    
    val orderSchema= StructType(List(
    
        StructField("country", StringType),
        StructField("WeekId", IntegerType),
        StructField("numInvoices", IntegerType),
        StructField("totalQuantity", IntegerType),
        StructField("invoiceValue", FloatType)
   
    ))
    
    
  val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .schema(orderSchema)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/windowdata.csv")
    .load()
    
//|InvoiceNo|StockCode|Description                        |Quantity|InvoiceDate    |UnitPrice|CustomerID|Country       |
    
//    ordersDf.selectExpr("count(*)  countofval").show
//     51
//  ordersDf.select(
//  count("*").as("countOfRecords"),
//  sum("Quantity").as("sumamt"),
//  countDistinct("InvoiceNo")
//  
//  ).show
//
//  ordersDf.selectExpr(
//  "count(*) countss"    
//  
//  ).show
//  
//  
//  ordersDf.groupBy("Country", "InvoiceNo")
//  .agg(expr("sum(Quantity) as sumopQuantity"),expr("sum(Quantity * UnitPrice) prices")).show
//  
  
    
    
//  ordersDf.createOrReplaceTempView("orders")
//  
//  spark.sql("""
//    
//    select *,sum(invoiceValue) over(partition by country order by weekId asc rows between unbounded preceding and current row) as runningtotal from orders
//    order by WeekId
//    
//    """).show
    
    val overValue = Window.partitionBy("Country").orderBy("weekId").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    ordersDf.withColumn("runningSum", sum("invoiceValue").over(overValue)).show
  
  
  
  
  
  
  
  
}