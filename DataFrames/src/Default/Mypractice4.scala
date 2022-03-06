import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._


object Mypractice4 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spConf = new SparkConf()
  spConf.set("spark.app.name", "men")
  spConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(spConf)
    .getOrCreate()
  
  val ageSchema = StructType(List(
      StructField("name", StringType),
      StructField("distance", IntegerType),
      StructField("city", StringType)

  ))

  def ageCheck(lines :Int): String={
    
    if(lines.toInt > 18) "Local" else "Non-Local"
  }
  
  val ageDf = spark.read
    .format("csv")
    .schema(ageSchema)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Assigniment/Age")
    .load()
  
  
    spark.udf.register("locality", ageCheck(_:Int))
    
    val newdf = ageDf.withColumn("islocalguy", expr("locality(distance)"))
    
    newdf.show
    
    
//   ageDf.createOrReplaceTempView("orders") 
//   
//   spark.sql("""
//     
//     select *,case when distance >18 then "Local"
//     else "nonLocak" end as islocalpeople from orders
//     
//     
//     """).show
   
    
}