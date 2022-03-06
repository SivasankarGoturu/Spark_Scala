package Practice

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object BoilerPlateCode extends App{
  
  
Logger.getLogger("org").setLevel(Level.ERROR)


val conf = new SparkConf()
conf.set("spark.master", "local[*]")
conf.set("spark.app.name", "mapp")


val spark = SparkSession.builder()
.config(conf)
.getOrCreate()


val studentRdd = spark.read
.format("csv")
.option("mode", "PERMISSIVE")
.option("header", true)
.option("inferSchema", true)
.option("path", "E:/DatasetToCompleteTheSixSparkExercises/students_with_header.csv")
.load()



studentRdd.write
.format("parquet")
.mode(SaveMode.Append)
.partitionBy("subject")
.option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFiles")
.save()



  
}