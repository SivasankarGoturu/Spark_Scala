import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.catalyst.expressions.Cast


object P_TwitterAnalysis extends App{
  
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf
  conf.set("spark.master", "local[2]")
  conf.set("spark.app.name", "Twitter Analysis")
  
  val spark = SparkSession.builder()
  .config(conf)
  .getOrCreate()
  
  import spark.implicits._
 
  val inputDf = spark.read
  .format("csv")
  .option("mode", "PERMISSIVE")
  .option("header", true)
  .option("path", "C:/Users/gotur/Documents/SparkDataSets/IRAhandle_tweets_1.csv")
  .load()
  
//  inputDf.show
  
  val mydf = inputDf.select(col("content"), col("publish_date")).withColumn("publish_date", to_date(col("publish_date"), "dd/MM/yyyy"))
  .withColumn("year", year(col("publish_date"))).drop(col("publish_date"))
  
  val finaldf = mydf.withColumn("content", split(col("content"), " ")).withColumn("content", explode(col("content"))).filter("content like '%#%' and content != ''")
  
  .withColumn("content", lower(col("content"))).withColumn("content", trim(col("content"))).withColumn("year", col("year").cast("int"))
  
  finaldf.createOrReplaceTempView("twitter")
  
//  spark.sql("""select content,year,count(*) val from twitter where year is not null group by content,year order by val desc""").show
//  

  spark.sql("""select content,year,count(*) over(partition by year) counting from twitter where year is not null order by counting desc""").show
    
    
//    ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    spark.sql("""select content,year,count(*) counting from twitter where year is not null group by content,year order by counting desc""").show
  
//    |        #trafficking|2017|       1|
    
//    |     #justtattooofus|2017|30919|
//  select content,year,count(*) over(partitioned by year order by year) from twitter
  
//  |            #maga|null|1789|
//|             #amb|null|1525|
  
//  |         #stlverdict|null|104987|
//|      #benatberkeley|null|104987|
// scala.io.StdIn.readLine()
  
  
//  +-----------------+----+----+
//|            #news|2015|1363|
//  |    #fightcensorship|2017|30919|
//  .groupBy(col("content")).agg(count(col("content")).as("numOfTweets")).orderBy(col("numOfTweets").desc)
  
  
//  .filter("content != ''")
//  .filter("content like '%#%'")
  
// val myrdd = mydf.rdd.map(x => x.mkString(","))
// 
// myrdd.take(20)
  
//  inputDf.drop(col("content")).withColumn("publish_date", to_date(col("publish_date"), "dd/MM/yyyy")).show
  
//    inputDf.select(col("region")).orderBy(col("region").desc).distinct().show
  
//  inputDf.select(col("region")).distinct().orderBy(col("region")).show(false)
  
  
}