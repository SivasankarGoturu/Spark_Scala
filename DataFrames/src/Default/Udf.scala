import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import scala.reflect.api.materializeTypeTag

object AssignimentQuestion extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spConf = new SparkConf()
  spConf.set("spark.app.name", "men")
  spConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(spConf)
    .getOrCreate()

  def ageCheck(age: Int) = {

    if (age > 18) "Y" else "N"
  }

  val ageDf = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Assigniment/Age")
    .load()

  import spark.implicits._

  val df1 = ageDf.toDF("name", "age", "city")

  // Column object expression

//  val regudf = udf(ageCheck(_: Int))
//
//  val df2 = df1.withColumn("IsAdult", regudf(col("age")))
//
//  df2.show
//  

  
// ========================================================================================================
  
  // SQL/ String expression udf
//
  spark.udf.register("regudf", ageCheck(_: Int))

    val df2 = df1.withColumn("adult", expr("regudf(age)"))
  
  df2.show
  
  // Using sql
  
//  df1.createOrReplaceTempView("peoples")
//
//  spark.sql("select name, age, city, regudf(age) as adult from peoples").show
//
//  spark.catalog.listFunctions().filter(x => x.name == "regudf").show

}