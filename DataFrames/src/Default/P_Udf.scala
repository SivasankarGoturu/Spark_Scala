import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object P_Udf extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "save data to table")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val ageDf = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Assigniment/Age")
    .load()

  import spark.implicits._

  val df1 = ageDf.toDF("name", "age", "city")

  def ageCheck(age: Int) = {

    if (age > 18) "Y" else "N"
  }

  // ================= Column Object Expression UDF ========================

//  val parseFun = udf(ageCheck(_: Int))
//
//  val resdf = df1.withColumn("IsMature", parseFun(col("age")))
//
//  resdf.show

  //=====================================================================
  
  // ================= String expression UDF ==========================
  
  
//  spark.udf.register("parseFun", ageCheck(_:Int))
//  
//  val resDf = df1.withColumn("IsAdult", expr("parseFun(age)"))
//  
//  resDf.show
  
  // ===================================================================
  
  // ============================ Sql expression UDF =====================
  
  spark.udf.register("parseFun", ageCheck(_:Int))
  
  df1.createOrReplaceTempView("cust")
  
  spark.sql("select name,age,city, parseFun(age) from cust").show
  
  
  
  
  
  
  
  
  
  
  

}