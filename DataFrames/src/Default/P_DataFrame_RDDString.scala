import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql._

object P_DataFrame_RDD extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val con = new SparkConf
  con.set("spark.app.name", "Conversions")
  con.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(con)
    .getOrCreate()

  val customerDF = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
    .load

  import spark.implicits._

  //       case class Customers(customer_id: String, customername: String)
  //
  //    def parser(lines :String)={
  //
  //    val fields = lines.split(",")
  //
  //    		val res = Customers(fields(0), fields(1))
  //
  //    		res
  //  }

  // RDD row
  val customerRDD = customerDF.rdd

  // RDD String
  val custrddString = customerRDD.map(_.mkString(","))

  customerRDD.take(10).foreach(println)
  
  custrddString.take(10).foreach(println)

}