//package Practice
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.DataFrame
//import org.apache.log4j.Level
//import org.apache.log4j.Logger
//
//object nilsoIQ {
//
//  Logger.getLogger("org").setLevel(Level.ERROR)
//
//  val conf = new SparkConf()
//  conf.set("spark.master", "local[2]")
//  conf.set("spark.app.name", "nilsonIQ")
//
//  val spark = SparkSession.builder()
//    .config(conf)
//    .getOrCreate()
//
//  import spark.implicits._
//
//  def extract_councils(): DataFrame = {
//
//    val districtDf = spark.read
//      .format("csv")
//      .option("header", true)
//      .option("inferSchema", true)
//      .option("mode", "PERMISSIVE")
//      .option("path", "E:/Newfolder/district_councils.csv")
//      .load
//
//    val df1 = districtDf.withColumn("council_type", lit("District Council"))
//
//    val londonDf = spark.read
//      .format("csv")
//      .option("header", true)
//      .option("inferSchema", true)
//      .option("mode", "PERMISSIVE")
//      .option("path", "E:/Newfolder/london_broughts.csv")
//      .load
//
//    val df2 = londonDf.withColumn("council_type", lit("London Broughts"))
//
//    val metropolitianDf = spark.read
//      .format("csv")
//      .option("header", true)
//      .option("inferSchema", true)
//      .option("mode", "PERMISSIVE")
//      .option("path", "E:/Newfolder/metropolitian_districts.csv")
//      .load
//
//    val df3 = metropolitianDf.withColumn("council_type", lit("Metropolitian Districts"))
//
//    val unitoryDf = spark.read
//      .format("csv")
//      .option("header", true)
//      .option("inferSchema", true)
//      .option("mode", "PERMISSIVE")
//      .option("path", "E:/Newfolder/unitory_authorities.csv")
//      .load
//
//    val df4 = unitoryDf.withColumn("council_type", lit("Unitory Authorities"))
//
//    val df5 = df1.union(df2).union(df3).union(df4)
//
//    df5
//
//  }
//
//  def extract_avg_price(df5: DataFrame): DataFrame = {
//
//    val avgPriceyDf = spark.read
//      .format("csv")
//      .option("header", true)
//      .option("inferSchema", true)
//      .option("mode", "PERMISSIVE")
//      .option("path", "E:/Newfolder/property_avg_price.csv")
//      .load
//
//    val avgOpt = avgPriceyDf.select(col("local_authoritye").as("council"), col("avg_price_nov_2019"))
//
//    avgOpt
//
//  }
//
//  def extract_sales_volume(avgOpt: DataFrame): DataFrame = {
//
//    val salesDf = spark.read
//      .format("csv")
//      .option("header", true)
//      .option("inferSchema", true)
//      .option("mode", "PERMISSIVE")
//      .option("path", "E:/Newfolder/property_sales_volume.csv")
//      .load
//
//    val salesOpt = salesDf.select(col("local_authority").as("council"), col("sales_volume_sept_2019"))
//
//    salesOpt
//  }
//
//  def transform(salesOpt: DataFrame) = {
//    df5.show()
//    salesOpt.show()
//
//  }
//
//  def main(args: Array[String]) {
//
//    extract_sales_volume(extract_avg_price(extract_councils()))
//
//  }
//}