package Practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType

object Process {
  def main(args: Array[String]) {
 
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.set("spark.master", "yarn")
    conf.set("spark.app.name", "filter Data")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    
    val ordersSchema = new StructType()
    .add("order_id", IntegerType)
    .add("order_date", TimestampType)
    .add("order_customer_id", IntegerType)
    .add("order_status", StringType)
    
    
    

    val inputDf = spark.read
      .format("csv")
      .option("header", true)
      .option("mode", "PERMISSIVE")
      .schema(ordersSchema)
      .option("path", args(0))
      .load()
      
//      "E:/DatasetToCompleteTheSixSparkExercises/orders.csv"

    val outputDf = inputDf.filter("order_status == 'COMPLETE'")
    
    outputDf.show

//    outputDf.write
//      .format("parquet")
//      .mode(SaveMode.Append)
//      .option("path", args(1))
//      .save()
      
//      "E:/DatasetToCompleteTheSixSparkExercises/outputFiles/prossedOpt/"
  }
}