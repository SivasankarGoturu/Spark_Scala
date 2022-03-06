import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object Week12 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.app.name", "solve")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()
    
    
        val employeeDf = spark.read
        .format("json")
        .option("mode", "PERMISSIVE")
        .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/Assigniment/employee.json")
        .load
        
        val deptDf = spark.read
        .format("json")
        .option("mode", "permissive")
        .option("path", "C:/Users/gotur/Documents/SparkDataSets/Week12/Assigniment/dept.json")
        .load()
        
//        employeeDf.show
//        
//        deptDf.show
        
        
        val newDeptDf = deptDf.withColumnRenamed("deptid", "depid")
        
        val joinCol = employeeDf.col("deptid") === newDeptDf.col("depid")
        
        val joinType = "left"
        
        val joinDf = newDeptDf.join(employeeDf, joinCol, joinType)
      
//          joinDf.show
        
        joinDf.groupBy("deptName","depid").agg(count("id").as("CountOfEmployees")).show
        
        
        

        
        
      
        
        
        
        
    
    
    
  
}