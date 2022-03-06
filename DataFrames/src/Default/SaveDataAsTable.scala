import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SaveDataAsTable extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "save data to table")
  conf.set("spark.master", "local[2]")

  //----Store temp meta store

  //  val spark = SparkSession.builder()
  //    .config(conf)
  //    .getOrCreate()

  // ----Store metadata perminently

  val spark = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferScema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/orders.csv")
    .load()

  //    ordersDf.write
  //    .format("csv")
  //    .mode(SaveMode.Overwrite)
  //    .saveAsTable("orders3")

      spark.sql("create database if not exists retail")
  //
  ordersDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(4, "order_customer_id")
    .sortBy("order_customer_id")
    .saveAsTable("retail.orders")

  spark.catalog.listTables("retail").show()
  //



  spark.stop()

}