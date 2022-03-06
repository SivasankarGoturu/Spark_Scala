package Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

object TimeStamps extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.master", "local[2]")
  conf.set("spark.app.name", "nilsonIQ")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val inputDf = spark.read
    .format("csv")
    .option("mode", "DROPMALFORMED")
    .option("header", true)
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/CAvideos.csv/*")
    .load()

  val reqDf = inputDf.select(col("title"), col("publish_time"), col("trending_date"), col("tags"), col("views").cast(LongType),
    col("likes").cast(LongType), col("dislikes").cast(LongType), col("comment_count").cast(LongType))

  val dateConvert = reqDf.withColumn("publish_time", col("publish_time").cast(TimestampType))
    .withColumn("trending_date", to_date(col("trending_date"), "yy.dd.MM")).persist(StorageLevel.MEMORY_AND_DISK)

  // Most viewed video
//  val dd = dateConvert.orderBy(col("views").desc).limit(1).drop("likes", "dislikes", "comment_count", "tags").show(false)

  //Most trending -- Based on least time taken to go for trending
  dateConvert.withColumn("timeForTrending", datediff(col("trending_date"), col("publish_time"))).orderBy(col("timeForTrending").asc_nulls_last)
  .show
  
  
  //  dateConvert.orderBy(col("views").desc).

  //      dateConvert.printSchema()

  spark.stop

}
