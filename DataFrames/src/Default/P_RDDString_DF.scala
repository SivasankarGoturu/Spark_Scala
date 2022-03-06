import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object P_RDDString_DF extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val con = new SparkConf
  con.set("spark.app.name", "Conversions")
  con.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(con)
    .getOrCreate()

    
      import spark.implicits._
  
    
    val customerRdd = spark.sparkContext.textFile("C:/Users/gotur/Documents/SparkDataSets/Week12/customers.csv")
    
    case class Customers(customer_id: String, customername: String)
    
    def parser(lines :String)={
    
    val fields = lines.split(",")
    
    		val res = Customers(fields(0), fields(1))
    
    		res
  }
    
    val custRdd = customerRdd.map(parser)
    
    val custDF = custRdd.toDF()
    
    
    custDF.show


}