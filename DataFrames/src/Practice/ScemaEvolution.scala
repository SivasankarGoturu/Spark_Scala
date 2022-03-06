package Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SaveMode

object ScemaEvolution extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.master", "local[2]")
  conf.set("spark.app.name", "ScemaEvolution")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val Li1 = List(
    (1, "Attr 0"), (2, "Attr 0"))

  val df1 = Li1.toDF("id", "att0")

  val Li2 = List(
    (1, "Attr 0", "Attr 1"), (2, "Attr 0", "Attr 1"))

  val df2 = Li2.toDF("id", "att0", "att1")

  val Li3 = List(
    (1, "Attr 1"), (2, "Attr 1"))

  val df3 = Li3.toDF("id", "att1")

  //  df1.write
  //  .format("parquet")
  //  .mode(SaveMode.Overwrite)
  //  .option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFile/file1")
  //  .save
  //
  //  df2.write
  //  .format("parquet")
  //  .mode(SaveMode.Overwrite)
  //  .option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFile/file2")
  //  .save
  //
  //  df3.write
  //  .format("parquet")
  //  .mode(SaveMode.Overwrite)
  //  .option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFile/file3")
  //  .save
  
  df1.show
  df2.show
  df3.show

  val inputDf = spark.read
    .format("parquet")
    .option("header", true)
    .option("mergeSchema", true)
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFile/*")
    .load
    
    inputDf.show()

}