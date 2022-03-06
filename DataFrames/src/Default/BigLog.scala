import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BigLog extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spConf = new SparkConf()
  spConf.set("spark.app.name", "men")
  spConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(spConf)
    .getOrCreate()

  import spark.implicits._

  // =============================================== Trail ===============================================
  
        case class Loggings(level: String, datetime: String)
  
        def mapper(lines: String) = {
  
          val fields = lines.split(",")
  
          val loggin = Loggings(fields(0),fields(1))
  
          loggin
        }
  
      val myList = List(
  "DEBUG,2015-2-6 16:24:07"
  ,"WARN,2016-7-26 18:54:43"
  ,"INFO,2012-10-18 14:35:19"
  ,"DEBUG,2012-4-26 14:26:50"
  ,"DEBUG,2013-9-28 20:27:13"
  ,"INFO,2017-8-20 13:17:27"
      )
  
      val logs = spark.sparkContext.parallelize(myList)
  
      val df1 = logs.map(mapper)
  
      val df2 = df1.toDF()
  
      df2.createOrReplaceTempView("logging_table")

      spark.sql("select * from logging_table").show

      spark.sql("select level, collect_list(datetime) from logging_table group by level order by level").show(false)

      spark.sql("select level, count(datetime) from logging_table group by level order by level").show(false)

      spark.sql("select level, date_format(datetime, 'MMMM') month ,count(*) from logging_table group by level,month").show
  
  // =========================================================================================================================
  
  // ========================================================== Actual Run ======================================================
  
//    val mydf = spark.read
//      .option("header", true)
//      .csv("C:/Users/gotur/Documents/SparkDataSets/Week12/biglog.txt")
//  
//    mydf.createOrReplaceTempView("logTable")
  
//    val results = spark.sql("""select level, date_format(datetime ,'MMM') month, count(1) as totoal from logTable
//         group by level, month order by month""").show(false)
  

    // OverCome month issue

//    val result1 = spark.sql("""select level, date_format(datetime ,'MMM') month,
//           cast(date_format(datetime, 'M') as int) monthNum,count(1) as totoal from logTable
//         group by level, month,monthNum order by monthNum, level""")
//  
//           val finalRes = result1.drop("monthNum")
//
//           finalRes.show
// -----------------------------------------------------------------------------------------------------------
           
  // =================================== Pivote Table ==========================================================
           
  
  //       System has to internally run a query to find distinct months and then showcase that in columns
         // So to avoid this we can hard code month values
  
//    spark.sql("""
//           select level, date_format(datetime ,'MMM') month,
//           cast(date_format(datetime, 'M') as int) monthNum from logTable""")
//           .groupBy("level").pivot("monthNum").count().show(100)
//  
           // Hard Code Month values
           
  
//         val columns = List("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
//  
//             spark.sql("""
//           select level, date_format(datetime ,'MMM') month
//            from logTable""")
//           .groupBy("level").pivot("month", "columns").count().show(100)

}