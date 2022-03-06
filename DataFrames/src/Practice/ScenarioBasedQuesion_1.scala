package Practice

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.Contains
import org.apache.spark.sql.types.IntegerType

object Solution extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.app.name", "Different Ways To Solve The Problem")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

    import spark.implicits._
    
  def parser(lines: String) = {

    val myarr = lines.split(",")
    val my_list = myarr.toList
    val f1 = my_list.head
    val f2 = my_list.tail.mkString(",")

    (f1, f2)
  }

  val myList = List(
    ("Row-Key-001, K1, 10, A2, 20, K3, 30, B4, 42, K5, 19, C20, 20"),
    ("Row-Key-002, X1, 20, Y6, 10, Z15, 35, X16, 42"),
    ("Row-Key-003, L4, 30, M10, 5, N12, 38, O14, 41, P13, 8"))

  val myrdd = spark.sparkContext.parallelize(myList)

  val myFun = myrdd.map(parser)

  val rdd1 = myFun.map(x => (x._1, x._2))

  val rdd2 = rdd1.flatMapValues(x => x.split(","))

  val rdd3 = rdd2.map(x => (x._1, x._2.trim()))
  
  val myDf = rdd3.toDF("key", "value")

  val filterDf = myDf.withColumn("value", trim(col("value"))).filter(col("value").rlike("[A-Z]"))
  
  val finalRdd = filterDf.rdd.map(x => x.mkString(","))
  
  finalRdd.collect().foreach(println)
  
  
  
  //  val filterDf = myDf.withColumn("value", trim(col("value"))).withColumn("value", col("value").cast(IntegerType)).filter(col("value").isNotNull)
  
  
//  mydf.filter(col("value").rlike("^[0-9]*$")).show
  
//  .withColumn("value", col("value").cast(IntegerType))
//  .rlike("^[0-9]*$")
//  rdd5.collect().foreach(println)
}