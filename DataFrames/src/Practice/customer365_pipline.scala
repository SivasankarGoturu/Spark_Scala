package Practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


object customer365_pipline {
  
  def main(args: Array[String]){
  
 Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf()
  conf.set("spark.master", "local[*]")
  conf.set("spark.app.name", "myPipeLine")
  
  val spark = SparkSession.builder()
  .config(conf)
  .getOrCreate()

  import spark.implicits._
  
  val inputDf = spark.read
  .format("csv")
  .option("mode", "PERMISSIVE")
  .option("header", true)
  .option("path", args(0))
  .load
  
  
  val finalDf = inputDf.filter("order_status == 'CLOSED'")
 

  
  
  finalDf.write
  .format("csv")
  .mode(SaveMode.Append)
  .option("path", args(1))
  .save()
  
  
//  command2 = """
//    spark-submit \
//    --master yarn \
//    --class Practice.customer365_pipline /home/sivacloud0027605/cust360.jar airflow-input/orders.csv airflow-output
//    """

  
  
  }
  
}