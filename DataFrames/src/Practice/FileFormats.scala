import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.FloatType


object SaveFormats extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spaek.app.name", "Save data to Parquet And Avro")
  sparkConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

  val windowSchema = StructType(List(

    StructField("country", StringType),
    StructField("weeknum", IntegerType),
    StructField("numinvoices", IntegerType),
    StructField("totalquantity", IntegerType),
    StructField("invoicevalu", FloatType)))

  val windowData = spark.read
    .format("csv")
    .schema(windowSchema)
    .option("header", true)
    .option("path", "C:/Users/gotur/Documents/SparkDataSets/Assigniment/windowdata.csv")
    .load()
    


//  windowData.write.partitionBy("country", "weeknum").parquet("C:/Users/gotur/Documents/ParquetFile")
  
//  windowData.write.partitionBy("country", "weeknum").orc("C:/Users/gotur/Documents/ORCFile")
    
//   windowData.write.format("avro").partitionBy("country").mode(SaveMode.Overwrite).option("path","C:/Users/gotur/Documents/AvroFile").save()
   
   windowData.write
.format("avro")
.partitionBy("Country")
.mode(SaveMode.Overwrite)
.option("path","C:/Users/gotur/Documents/AvroFile")
.save()
  


}