import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Ghhh extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spConf = new SparkConf()
  spConf.set("spark.app.name", "men")
  spConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(spConf)
    .getOrCreate()

  import spark.implicits._

  val arrayData = Seq(

    Row(1, "James", List("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown")),
    Row(2, "Michael", List("Spark", "Java", null), Map("hair" -> "brown", "eye" -> null)),
    Row(3, "Robert", List("CSharp", ""), Map("hair" -> "red", "eye" -> "")),
    Row(4, "Washington", null, null),
    Row(5, "", List(""), Map()))

  val arrayRdd = spark.sparkContext.parallelize(arrayData)

  val arraySchema = new StructType()
  .add("Id", IntegerType)
    .add("name", StringType)
    .add("Languages", ArrayType(StringType))
    .add("Properties", MapType(StringType, StringType))

  val newDf = spark.createDataFrame(arrayRdd, arraySchema)

  //  newDf.select($"name", explode($"Languages").alias("languagesKnow")).show()
  
   newDf.printSchema()

  val resDf = newDf.selectExpr("Id","name", "explode_outer(Languages) Languages")
  
  val fixDf = resDf.withColumn("Languages", expr("coalesce(Languages ,9999)"))

  val finalDf = fixDf.filter(x => !(x.mkString("").isEmpty() && x.length >0))

  finalDf.show

  spark.stop()

}