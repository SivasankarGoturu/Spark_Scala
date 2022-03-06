import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SimpleJoin extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "My spark App")
  sparkConfig.set("spark.master", "local[4]")

  val spark = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

  val customersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
    .load

  val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/orders.csv")
    .load

  // Before Join  ============================================================================================================================

  //    val newOrdersDf = ordersDf.withColumnRenamed("customer_id", "custid")
  //
  //  val joinCondition = customersDf.col("customer_id") === newOrdersDf.col("custid")
  //
  //  val joinType = "left"
  //
  //
  //
  //  val joinDf = newOrdersDf.join(customersDf, joinCondition, joinType)
  //    .select("customer_id", "order_date", "customer_city").show

  // ============================================================================================================================

    // After join usint .drop
    
  val joinCondition = customersDf.col("customer_id") === ordersDf.col("customer_id")

  val joinType = "outer"

  val joinDf = ordersDf.join(customersDf, joinCondition, joinType)
    .drop(ordersDf.col("customer_id"))
    .select("order_id", "customer_id", "customer_fname")
    .sort("order_id").show

}