package Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object Sql extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.master", "local[2]")
  conf.set("spark.app.name", "nilsonIQ")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  //  val productList = List(
  //    (1, "Scooter", 10),
  //    (2, "Bike", 20),
  //    (3, "Car", 30))
  //
  //  val salesList = List(
  //    (1, 1, 1),
  //    (2, 2, 1),
  //    (3, 2, 1),
  //    (4, 2, 1))
  //
  //  val productDf = productList.toDF("PID", "Name", "Price")
  //  val salesDf = salesList.toDF("ID", "PID", "QTY")
  //
  //  productDf.show
  //  salesDf.show
  //
  //  val joinCondition = productDf.col("PID") === salesDf.col("PID")
  //  val joinType = "left"
  //
  //  val joiningDf = productDf.join(salesDf, joinCondition, joinType)
  //
  //  joiningDf.groupBy(productDf.col("PID")).agg(sum(productDf.col("Price") * salesDf.col("QTY"))).show
  //
  //  productDf.createOrReplaceTempView("product")
  //  salesDf.createOrReplaceTempView("sales")
  //
  //
  //
  //  spark.sql("""
  //
  //  select p.PID, sum(QTY*Price) from product p
  //  left join sales s on s.PID = p.PID
  //  group by p.PID
  //
  //  """).show

  //  val myList = List("5.34","2.34","38.09","737")

  //  myList.toDF("col1").withColumn("col1", split(col("col1"), ".")).show

  //  .withColumn("col2", substring_index(col("col1"), ".", -1)).show

  val inputDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE") // FAILFAST // DROPMALFORMED
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/bigLogNew.txt")
    .load()
    
    
    inputDf.write
    .format("csv")
    .mode(SaveMode.Append) //append// overwrite//errorifexist // ignore
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFile/")
    .save()
    
    
    scala.io.StdIn.readLine()
    
    
    
    
    
    
    
    
    
    
    

}