import java.sql.Timestamp

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Learn extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class orders(order_id: Int, order_date: Timestamp, order_customer_id: String, order_status: String)

  val con = new SparkConf()
  con.set("spark.app.name", "men")
  con.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(con)
    .getOrCreate()

  val ordersDf = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("C:/Users/gotur/Documents/SparkDataSets/DataSets/orders.csv")

  import spark.implicits._

  val ordersDs = ordersDf.as[orders]

  //    ordersDs.filter(x => x.order_id <10).show

  //    ordersDf.filter(x => x.order_id <10).show()    // Dont work for dataframes

//  ordersDs.filter("order_id < 10").show
//  
//  ordersDs.toDF().show()
  
val myrd = ordersDs.rdd




  
//  val logg = spark.sparkContext.parallelize(ordersDf)
  

}