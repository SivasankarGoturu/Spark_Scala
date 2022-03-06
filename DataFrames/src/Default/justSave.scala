import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level


object justSave {
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf
    conf.set("spark.app.name", "just save to HDFS location")
    conf.set("spark.master", "yarn")
    
    val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()
    
    val inputDf = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("header", true)
    .option("path", args(0))
    .load
    
    
    inputDf.write
    .format("csv")
    .mode(SaveMode.Append)
    .option("path", args(1))
    .save()
    
    
    spark.stop()
    
    
    
  }
  
}