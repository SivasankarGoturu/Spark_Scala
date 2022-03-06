import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.expressions.Window

object DifferentWaysToSolve extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.app.name", "Different Ways To Solve The Problem")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val ordersSchema = StructType(List(

    StructField("InvoiceNo", IntegerType),
    StructField("StockCode", StringType),
    StructField("Description", StringType),
    StructField("Quantity", IntegerType),
    StructField("InvoiceDate", StringType),
    StructField("UnitPrice", FloatType),
    StructField("CustomerID", StringType),
    StructField("Country", StringType)))

  val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/order_data.csv")
//    .schema(ordersSchema)
    .load()

  // ============================================= Simple Aggregates ========================================================
  /*

    // Column Object Type

  ordersDf.select(

    count("*").as("CountofRecords"),
    sum("Quantity").as("TotalQuantity"),
    avg("UnitPrice").as("AvgOfUnits"),
    countDistinct("InvoiceNo").as("DistinctInvoices")).show


    //Column String Type

ordersDf.selectExpr(

    "count(*)  Countofrec",
    "sum(Quantity) as TotalQuantity",
    "avg(UnitPrice) as AvgOfUnits",
    "count(distinct InvoiceNo) as DistinctInvoices"

).show

  // Spark Sql

ordersDf.createOrReplaceTempView("salesTable")

spark.sql("select count(*) countofrecords, sum(Quantity) SumofQuantity,avg(UnitPrice) aveofunitprice, count(distinct InvoiceNo) as DistinctInvoice from salesTable").show

*/

  // =============================================================================================================================================================

  //=============================================================== Grouping Aggregates ==========================================================================
  /*

     // Column Object

  val summaryDf = ordersDf.groupBy("Country", "InvoiceNo")
    .agg(sum("Quantity").as("totalQuantity"), sum(expr("Quantity * UnitPrice")).as("totalAmount"))


  summaryDf.show

		// Column String

   val summaryDf2 = ordersDf.groupBy("Country", "InvoiceNo")
   .agg(expr("sum(Quantity) as totalQuantity"), expr("sum(Quantity * UnitPrice) as totalQuantity"))

    summaryDf2.show

   // Spark SQL

   ordersDf.createOrReplaceTempView("salesTable")

   val summaryDf3 = spark.sql(
       """select Country, InvoiceNo, sum(Quantity) totalQuantity, sum(Quantity * UnitPrice) totalQuantity from salesTable
     group by Country,InvoiceNo""")

      summaryDf3.show


  */

  // ==================================================== Window Aggregate ======================================================================================

  //  ordersDf.show
    
   
    
//    ordersDf.withColumn()
    

  spark.stop()

}