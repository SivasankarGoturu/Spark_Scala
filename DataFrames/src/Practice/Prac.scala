import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object type_name extends {

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
    
    val customerRDD = customerDF.toDF()
    
    
    
    
    
//    val custdf = spark.sparkContext.textFile("C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
//    
//    custdf.toDF()
    

}