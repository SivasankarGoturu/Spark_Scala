import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object My extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConfig = new SparkConf()
  sparkConfig.set("spark.app.name", "My spark App")
  sparkConfig.set("spark.master", "local[4]")

  val spark = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()
    
    import spark.implicits._

  val mydf = spark.read
      .option("header", true)
      .csv("C:/Users/gotur/Documents/SparkDataSets/Week12/biglog.txt")
  
val myr = mydf.rdd.map(_.mkString(","))

myr.toDS() 
  
 mydf.createOrReplaceTempView("biglog")
 
 spark.sql("""select level, cast(date_format(datetime, 'M') as int) month, count(level) countOfLogs from biglog
   group by level, date_format(datetime, 'M') order by month
   """).show
 
 
}