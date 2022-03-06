import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object ReferAColumn extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "save data to table")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferScema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/orders.csv")
    .load()
    
    // Column String
    
//    ordersDf.select("order_date", "order_id").show()
    
    // Column Object
    
    import spark.implicits._
    
    ordersDf.select(column("order_date"), col("order_status"), $"order_customer_id",'order_Status).show
    
    
    
    ordersDf.select(col("order_id"),expr("concat(order_status,'_STATUS')")).show(false)
    
    ordersDf.selectExpr("order_id", "order_date","concat(order_status,'_STATUS')").show(false)
    
    
    ordersDf.show(false)

}