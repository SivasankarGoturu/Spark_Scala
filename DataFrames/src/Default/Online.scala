import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.CharType


object Online extends App{
  
    Logger.getLogger("org").setLevel(Level.ERROR)

  val con = new SparkConf
  con.set("spark.app.name", "Conversions")
  con.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(con)
    .getOrCreate()
  
  
   val data = Seq(("James, A, Smith","2018","M",3000),
    ("Michael, Rose, Jones","2010","M",4000),
    ("Robert,K,Williams","2010","M",4000),
    ("Maria,Anne,Jones","2005","F",4000),
    ("Jen,Mary,Brown","2010","",-1)
  )
  
val dataSchema = new StructType()
    .add("name", StringType)
    .add("dob", IntegerType)
    .add("sex", StringType)
    .add("sal", IntegerType)
  
    val data1 = spark.sparkContext.parallelize(data)
    
//  val mydf = spark.createDataFrame(data1, dataSchema)
  
//  val mysplit = mydf.selectExpr("name", "explode(name)").show
  
//  mydf.show
  
}