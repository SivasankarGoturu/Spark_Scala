import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object Mypractice5 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "My spark App")
  sparkConfig.set("spark.master", "local[4]")

  val spark = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("inferSchema", true)
    .option("header", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/orders.csv")
    .load

  val customersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("inferSchema", true)
    .option("header", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
    .load
    
    
    
    val newordersDf = ordersDf.withColumnRenamed("order_customer_id", "customer_id")
    
   
    
    val finalOrders = newordersDf.withColumnRenamed("customer_id", "custid")
    
//     finalOrders.show
//    customersDf.show
//    
    val joinCondition = finalOrders.col("custid") === customersDf.col("customer_id")
    
    val joinType = "left"
    
    
    val joinTable = customersDf.join(finalOrders, joinCondition, joinType)
    
    joinTable.selectExpr("order_id", "customer_id", "customer_fname")
    .where("order_id is NULL")
    .withColumn("order_id", expr("coalesce(order_id, -1)")).show
    
//    ordersDf.createOrReplaceTempView("orders")
//    
//    customersDf.createOrReplaceTempView("customers")
    
//    spark.sql("select customer_id,customer_fname from customers c left join orders o on o.order_customer_id = c.customer_id where order_customer_id is NULL").show
    
    
    
    
    
    

}