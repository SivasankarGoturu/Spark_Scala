package Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkConf

object ETL_PipeLine {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "Clean Json Data")
  conf.set("spark.master", "local[2]")
  conf.set("spark.sql.shuffle.partitions", "2")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  //Create External Schema
  val electricSchema = new StructType()
    .add("ChargingEvent", LongType)
    .add("CPID", StringType)
    .add("StartDate", StringType)
    .add("StartTime", StringType)
    .add("EndDate", StringType)
    .add("EndTime", StringType)
    .add("Energy", FloatType)
    .add("PluginDuration", FloatType)

  import spark.implicits._

  // Extract data from csv file
  def extract(): DataFrame = {

    // 1. Extract the data
    val electricDf = spark.read
      .format("csv")
      .option("mode", "PERMISSIVE")
      .option("header", true)
      .schema(electricSchema)
      .option("path", "E:/DatasetToCompleteTheSixSparkExercises/electric-chargepoints-2017.csv")
      .load()

//    println("************ Input Schema ************")
//    electricDf.printSchema()
//    println("************ Input data ************")
//    electricDf.show
//    val part1 = electricDf.rdd.getNumPartitions
//    println(s"Original num of partitons: $part1")

    electricDf

  }

  // Transformations
  def transform(input: DataFrame): DataFrame = {

    // 2. Rounding "PluginDuration" to 2 decimal points
    val roundingDf = input.withColumn("PluginDuration", round(col("PluginDuration"), 2))

    // 3. concatinating "StartDate |StartTime |  EndDate | EndTime" columns
    val concatDate = roundingDf.withColumn("pugInTime", concat(col("StartDate"), lit(" "), col("StartTime")))
      .withColumn("pugOutTime", concat(col("EndDate"), lit(" "), col("EndTime")))
      .drop(col("StartDate"))
      .drop(col("StartTime"))
      .drop(col("EndDate"))
      .drop(col("EndTime"))

    // 4. Making to DateTime format.

    val toDateTime = concatDate.withColumn("pugInTime", col("pugInTime").cast("timestamp"))
      .withColumn("pugOutTime", col("pugOutTime").cast("timestamp"))

    val part1 = toDateTime.rdd.getNumPartitions
    println(s"num partitons: $part1")

    // 5. Finding aggregated values
    val aggregate = toDateTime.groupBy(col("CPID"))
      .agg(avg(col("PluginDuration")).alias("avg_duration"), max(col("PluginDuration")).alias("max_duration"), sum(col("Energy")).as("energy_consumption"))

    val part2 = aggregate.rdd.getNumPartitions
    println(s"Partitions after first aggregation: $part2")

    val finalOutput = aggregate.select(col("CPID"), round(col("avg_duration"), 2).alias("avg_duration"), col("max_duration"), round(col("energy_consumption"), 2).as("energy_consumption"))
      .orderBy(col("energy_consumption").desc)

    val part3 = finalOutput.rdd.getNumPartitions
    println(s"Partitions after second aggregation: $part3")

    finalOutput

  }

  def load(output: DataFrame) = {

//    println("********* Output Schema *********")
//    output.printSchema()
//    println("********* Output Data *********")
//    output.show

    val part3 = output.rdd.getNumPartitions
    println(s"Partitions in final data: $part3")

    // Save into PARQUET file format
    output.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFiles/mydata")
      .save

  }

  def main(args: Array[String]) {

    load(transform(extract()))
    
    scala.io.StdIn.readLine()
    spark.stop()

  }

}