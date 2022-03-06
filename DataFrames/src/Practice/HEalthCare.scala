package Practice

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

object HEalthCare extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.master", "local[2]")
  conf.set("spark.app,name", "health care stroke")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val inputDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week13/bigLogNewtxt/bigLogNew.txt")
    .load
    
   val outputDf = inputDf.toDF("ErrorData").withColumn("ErrorData", split(col("ErrorData"), ": ")).select(col("ErrorData").getItem(0).as("error"), col("ErrorData").getItem(1).as("errorDetails"))
    
   outputDf.write
   .format("parquet")
   .mode(SaveMode.Append)
   .option("compression", "snappy")
   .option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFile")
   .save
   
//    healthDf.write
//    .format("parquet")
//    .mode(SaveMode.Append)
//    .option("path", "")

//  val df1 = healthDf.persist(StorageLevel.MEMORY_ONLY)

//  df1.show

//  df1.createOrReplaceTempView("strokes")
  
//  |   id|gender| age | hypertension|heart_disease|ever_married|    work_type|Residence_type|avg_glucose_level| bmi| smoking_status|stroke|

//  spark.sql("""select work_type,count(stroke) Counts_Of_Strokes from strokes 
//      where stroke =1
//      group by work_type
//      order by Counts_Of_Strokes desc""").show
//
//  df1.where("stroke=1").groupBy("work_type").agg(count("stroke").as("Counts_Of_Strokes")).orderBy(col("Counts_Of_Strokes").desc).show()
//  
//  df1.groupBy("gender").agg(count("gender"), (count("gender")*100/count("100 ")))

  
  
//  scala.io.StdIn.readLine()

}