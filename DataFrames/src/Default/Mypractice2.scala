import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object Mypractice2 extends App{
  
   Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "save data to table")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()
    
    val myregex = """^(\S+) (\S+)\t(\S+),(\S+)""".r
    
    case class ordersClass(orderid :String, date: String, customerid :String, status :String)
    
    def parser(lines :String) ={
     
     lines match {
       
       case myregex("orderid", "date", "customerid", "status") => ordersClass("orderid", "date", "customerid", "status")
       
     }
     
   }
    
    val ordersDf = spark.read
    .format("csv")
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/orders_new.csv")
    .load()
    
      import spark.implicits._
    
    val ordersRdd = ordersDf.rdd
    
    val ss = ordersRdd.map(_.mkString(","))
    
    val dd = ss.toDS()
    
    val ty = dd.rdd
    
//    ordersRdd.collect().foreach(println)
    
//    val rdds = ordersRdd.map(parser)
    
  
}