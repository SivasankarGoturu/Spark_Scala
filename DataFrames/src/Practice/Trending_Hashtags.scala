//package Practice
//
//import org.apache.log4j.Level
//import org.apache.log4j.Logger
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql._
//import org.apache.spark.sql.expressions.Window
//
//object Trending_Hashtags extends App {
//
//  Logger.getLogger("org").setLevel(Level.ERROR)
//
//  val conf = new SparkConf
//  conf.set("spark.master", "local[2]")
//  conf.set("spark.app.name", "twitterTweets")
//
//  val spark = SparkSession.builder()
//    .config(conf)
//    .getOrCreate()
//
//      val inputSchema = new StructType()
//    .add("", dataType, nullable)
//
//  val inputDf = spark.read
//    .format("parquet")
//    .option("mode", "PERMISSIVE")
//    .option("header", true)
//    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/twitter/*")
//    .load()
//
//  val reqFields = inputDf.select(col("tweet_id"), col("content"), col("publish_date"))
//
//      reqFields.show
//
//  val df2 = reqFields.withColumn("publish_date", to_timestamp(col("publish_date"), "dd/MM/yyyy hh:mm"))
//    .filter("publish_date not like 'null'")
//    .withColumn("content", split(col("content"), " "))
//    .withColumn("content", explode(col("content")))
//    .filter("content like '#%'")
//    .withColumn("content", lower(col("content")))
//    .withColumn("publish_date", year(col("publish_date")))
//    .withColumn("content", trim(col("content")))
//    
////    df2.write
////    .format(".csv")
////    .mode(SaveMode.Append)
////    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/outputFile/hello1")
////    .save()
//    
//    
//    
//
//    
//    
//    
//    
//    
//    
//    
//    
//    
//    
//    
//    
//    
//    
//    
//    
//
//  //    df2.printSchema()
//
//  //    df2.show
//  //
//  //    val myWin = Window.partitionBy(col("publish_date"))
//  //    .orderBy(col("numTweets").desc)
//  //    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
//
//  //    df2.withColumn("top10", count("content").over(Window.partitionBy(col("publish_date"))
//  //    .orderBy(col("top10").desc)
//  //    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).show
//
////  df2.createOrReplaceTempView("tweets")
//
//  //    spark.sql("""
//  //
//  //      select content,publish_date,count(*) TweetCount from tweets
//  //      group by content,publish_date
//  //      order by TweetCount desc
//  //
//  //      """).show
//
//  //      select content,publish_date,count(*) over(partition by publish_date order by publish_date rows between unbounded preceding and current row) TweetCount from tweets
////  spark.sql("""
////      
////  select *,count(content) over(partition by publish_date order by content rows between unbounded preceding and current row) counts from tweets
////
////      
////      """).show
//      
////        select content,publish_date,count(*) tw from tweets 
//// where content = '#maga'
////  group by content,publish_date
////
////  order by tw desc
//
//  //  spark.sql("""""")
//
//  //    inputDf.show()
//
//  //      '#maga'
//  //      |     519|
//      
////      +-------+------------+---+
////|content|publish_date| tw|
////+-------+------------+---+
////|  #maga|        2017|374|
////|  #maga|        2016|144|
////|  #maga|        2018|  1|
////+-------+------------+---+
//
//}