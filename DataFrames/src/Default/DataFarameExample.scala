import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level



object DataFarameExample extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spConf = new SparkConf()
  spConf.set("spark.app.name", "men")
  spConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(spConf)
    .getOrCreate()

  val ordersDf = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("C:/Users/gotur/Documents/SparkDataSets/DataSets/orders.csv")

  val result = ordersDf.repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id", "order_customer_id")
    .groupBy("order_customer_id")
    .count()

  //  result.foreach(x => {println(x)})  -- Low level condtruct

  result.show()

//  scala.io.StdIn.readLine()

  spark.stop()
  
}