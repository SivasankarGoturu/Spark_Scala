import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "spark SQL")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/orders.csv")
    .load()

  ordersDf.createOrReplaceTempView("orders")

  //  val resultDf = spark.sql("select order_status,count(*) statusCount from orders group by order_status order by statusCount desc")

  val resultDf = spark.sql("select order_customer_id,count(*) totalorders from orders " +
    "where order_status = 'CLOSED'group by order_customer_id order by totalorders desc")

  resultDf.show()

}