import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object Replace extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spConf = new SparkConf()
  spConf.set("spark.app.name", "men")
  spConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(spConf)
    .getOrCreate()

  //Create DataFrame
  val address = Seq(
    (1, "14851 Jeffrey Rd", "DE"),
    (2, "43421 Margarita St", "NY"),
    (3, "13111 Siemon Ave", "CA"))

  import spark.implicits._

  val df = address.toDF("id", "address", "state")

  //Replace part of a string using regexp_replace()
  import org.apache.spark.sql.functions.regexp_replace
  df.withColumn("address", regexp_replace($"address", "14851 Jeffrey Rd", "Road")).show()

}