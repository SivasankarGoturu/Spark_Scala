import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._

object P_Split extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf
  conf.set("spark.master", "local[2]")
  conf.set("spark.app.name", "Spliting values")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

    import spark.implicits._
    
  val data = List(
    ("James, A, Smith", "2018", "M", 3000),
    ("Michael, Rose, Jones", "2010", "M", 4000),
    ("Robert,K,Williams", "2010", "M", 4000),
    ("Maria,Anne,Jones", "2005", "F", 4000),
    ("Jen,Mary,Brown", "2010", " ", -1))


  val df = data.toDF("name", "dob_year", "gender", "salary")
  df.printSchema()
  df.show(false)
  
  val df2 = df.withColumn("name" ,split(col("name"),","))

    df2.show
    
//    df2.withColumn("firstName", col("name").getItem(0)).show
//    
//    val df3 = df2.withColumn("name", explode(col("name"))).withColumn("name", trim(col("name")))
//    
//    df3.show
//    
//    df3.printSchema()
//    
//    df3.createOrReplaceTempView("names")
//    
//    spark.sql(""" select name,count(name) from names  group by name""").show
    
    
}