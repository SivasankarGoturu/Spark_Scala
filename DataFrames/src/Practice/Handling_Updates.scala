package Practice

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Handling_Updates extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.master", "local[2]")
  conf.set("spark.app.name", "ScemaEvolution")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._
//  
//  StructField("order_no", StringType, true),
//    StructField("customer_id", StringType, true),
//    StructField("quantity", IntegerType, true),
//    StructField("cost", DoubleType, true),
//    StructField("order_date", DateType, true),
//    StructField("last_updated_date", DateType, true)
  
//  order_no	customer_id	quantity	cost	order_date	last_updated_date
//001	u1	1	$15.00	03/01/2020	03/01/2020
//002	u2	1	$30.00	04/01/2020	04/01/2020
//
//
//order_no	customer_id	quantity	cost	order_date	last_updated_date
//002	u2	1	$20.00	04/01/2020	04/02/2020
  
  val Li1 = List(
  ("001", "u1", 1, "$15.00", "03/01/2020", "03/01/2020"),
  ("002", "u2", 1, "$30.00", "04/01/2020", "04/01/2020")
  )
  
  val ordersDf1 = Li1.toDF("order_no",	"customer_id",	"quantity", "cost", "order_date", "last_updated_date")
  
  val ordersDf = ordersDf1.withColumn("order_date", to_date(col("order_date"), "dd/MM/yyyy"))
  .withColumn("last_updated_date", to_date(col("last_updated_date"), "dd/MM/yyyy"))
  
  val Li2 = List(
  ("002", "u2", 1, "$20.00", "04/01/2020", "04/02/2020")
  )
  
  val ordersUpdateDf1 = Li2.toDF("order_no",	"customer_id",	"quantity", "cost", "order_date", "last_updated_date")
  
  val ordersUpdateDf = ordersUpdateDf1.withColumn("order_date", to_date(col("order_date"), "dd/MM/yyyy"))
  .withColumn("last_updated_date", to_date(col("last_updated_date"), "dd/MM/yyyy"))
  

//  ordersDf.show()
//  ordersUpdateDf.show()
  
  val unionDf = ordersDf.union(ordersUpdateDf)
  
//  unionDf.show
  
  val myWin = Window.partitionBy("order_no").orderBy(col("last_updated_date").desc)
  
  unionDf.withColumn("rankingCol", dense_rank().over(myWin))
  .where("rankingCol = 1").drop(col("rankingCol")).show
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
//  val groupedDf = unionDf.groupBy("order_no",	"customer_id",	"quantity", "cost", "order_date")
//  .agg(
//      max("last_updated_date").as("updatedValue")
//  )
//  
//  val joinCondition = col("groupedDf.updatedValue") === col("unionDf.last_updated_date")
//  
//  val joinType = "inner"
//  
//  unionDf.join(groupedDf, joinCondition, joinType).show
  
  
  
}