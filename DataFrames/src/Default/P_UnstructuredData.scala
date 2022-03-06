import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object P_UnstructuredData extends App {

 Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "save data to table")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val myregx = """^(\S+) (\S+)\t(\S+),(\S+)""".r

  case class Orders(orderId: Int, custId: Int, Status: String)

  def parser(lines: String) = {

    lines match {
      case myregx(orderId, orderDate, custId, orderStatus) => 
        Orders(orderId.toInt, custId.toInt, orderStatus)
    }
  }


  val orderRdd = spark.sparkContext.textFile("C:/Users/gotur/Documents/SparkDataSets/Week12/orders_new.csv")


    import spark.implicits._
    
  val ordersDS = orderRdd.map(parser).toDS().cache()
  
  ordersDS.select("orderId")


}