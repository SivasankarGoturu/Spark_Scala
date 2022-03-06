import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.DateType

object P_JsonDataClean extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "Clean Json Data")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val inputJson = spark.read
    .format("json")
    .option("mode", "PERMISSIVE")
    .option("multiline", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/sample2.json")
    .load()

  inputJson.printSchema()

  inputJson.show()
  
  inputJson.select(col("address.city")).show
  
  // Flattening the array using explode() function
  val explodingDf = inputJson.withColumn("phoneNumbers", explode(col("phoneNumbers")))

  explodingDf.printSchema()
  // Accesing elements in array
  val finalOutput = explodingDf.select(col("firstName"),col("lastName"),col("gender"),col("age"),col("address.*"),col("phoneNumbers.*"))
  
  finalOutput.printSchema()
  
  finalOutput.show
  
  
  
  
  
  
  
  
    spark.stop
  
}