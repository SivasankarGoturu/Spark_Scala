package Practice

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.apache.parquet.format.DateType
import org.apache.spark.sql._
import org.apache.spark.sql.types.LongType

object Counting {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "Clean Json Data")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

//  val ordersSchema = StructType(List(
//
//    StructField("ChargingEvent", IntegerType),
//    StructField("CPID", StringType),
//    StructField("StartDate", TimestampType),
//    StructField("StartTime", StringType),
//    StructField("EndDate", TimestampType),
//    StructField("EndTime", StringType),
//    StructField("Energy", FloatType),
//    StructField("PluginDuration", FloatType)))

    
    def extract(): DataFrame ={
      val ordersDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
//    .schema(ordersSchema)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/electric-chargepoints-2017.csv")
    .load()
    
    ordersDf 
    
  }
    def transform(df: DataFrame): DataFrame ={
      
      df
      
    }
    
    def load(df2: DataFrame) ={
      
    df2.show
    
    val df3 = df2.withColumn("starting", concat(col("StartDate").cast("date"), lit(" "), col("StartTime"))).drop("StartDate").drop("StartTime")
    .withColumn("ending", concat(col("EndDate").cast("date"), lit(" "), col("EndTime"))).drop("EndDate").drop("EndTime")
      
    val df4 = df3.withColumn("starting", col("starting").cast("timestamp")).withColumn("ending", col("ending").cast("timestamp"))
    
    df4.show(false)
    
    df4.printSchema()
    
    val df5 = df4.withColumn("datediff", datediff(col("ending"), col("starting")))
    .withColumn("monthsdiff", round(months_between(col("starting"), col("ending")),2))   //
    
    
   val df6 = df5.withColumn("diffSecs", col("ending").cast(LongType) - col("starting").cast(LongType))
    .withColumn("diffMints", col("diffSecs")/60)
    .withColumn("diffHours", col("diffSecs")/3600)
    .withColumn("diffDays", col("diffSecs")/(3600*24))
    .withColumn("diffDays", round(col("diffDays"),2))

    df6.show
    
       df6.printSchema()
    
   
//       df6.write
//       .format("parquet")
//       .mode(SaveMode.Append)
//       .option("path", "C:/Users/gotur/Documents/SparkDataSets/der/myoutput")
//       .save
      
    }
    
 def main(args: Array[String]){
    
    
    load(transform(extract()))
    
    
    
  }   
    
}