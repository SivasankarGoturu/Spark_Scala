import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.functions._

object Iiiii extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "Clean Json Data")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val mydf = spark.read
    .format("json")
    .option("multiLine", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/sample2.json")
    .load()

  mydf.show(false)

  mydf.printSchema()

//  mydf.withColumn("phoneNumber", explode($"phoneNumbers")).show(false)
  
//  mydf.select($"age",explode($"phoneNumbers").as("phone")).show()

  val mydf2 = mydf.withColumn("phoneNumber", explode($"phoneNumbers")).drop("phoneNumbers")

  val mydf3 = mydf2.select($"firstName", $"lastName", $"age", $"gender", $"address.*", $"phoneNumber.number")

//  val df4 = mydf3.withColumnRenamed("number", "PhoneNumber")

  mydf3.show

  mydf3.printSchema()

  spark.stop

}