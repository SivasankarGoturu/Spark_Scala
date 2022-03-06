import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._

object DataFrame_DataSet extends App {

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
    //    .option("inferSchema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
    .load

  import spark.implicits._

  // We cant change column names in case class, because we are mapping with original df

  case class customers(customer_id: String, customer_fname: String, customer_lname: String, customer_email: String, customer_password: String, customer_street: String, customer_city: String, customer_state: String, customer_zipcode: String)
  
 
  val custds = customerDF.as[customers]

  custds.show()

}