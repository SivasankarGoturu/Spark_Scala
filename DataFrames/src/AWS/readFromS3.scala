package AWS

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

object type_name {
  def main(args: Array[String]) {

    //Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf()
      .set("spark.app.name", "connect to s3")
      .set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAYNNZNMZMLNCPCQNN")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val df = spark.read.format("csv").option("path", "s3a://onlinesbi/orders.csv").load
    
    df.show
    
  }
}