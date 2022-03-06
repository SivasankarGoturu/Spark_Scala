package Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object ScenarioBasedQuestion_2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.app.name", "Different Ways To Solve The Problem")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val inputDf = spark.read
    .format("json")
    .option("multiline", true)
    .option("header", true)
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/inputFile.json")
    .load

  inputDf.printSchema()

  val reqDF = inputDf.toDF()
  reqDF.show

  val df1 = reqDF.select(col("dataset.company.address").as("address"), col("dataset.company.employees").as("employees"), col("dataset.company.name").as("Company"),
    col("dataset.company.noOfEmployees").as("noOfEmployees"), col("dataset.company.year").as("year"))

  val finalOut = df1.withColumn("employees", explode(col("employees"))).withColumn("id", col("employees").getItem("id"))
    .withColumn("name", col("employees").getItem("name")).drop("employees")

  finalOut.select(col("Company"), col("address"), col("noOfEmployees"), col("id").as("employees_id"), col("name").as("employees_name"), col("year")).show
}