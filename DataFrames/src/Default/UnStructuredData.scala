import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object UnStructuredData extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
  conf.set("spark.app.name", "save data to table")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r

  case class Orders(order_id: Int, customer_id: Int, order_status: String)

  def parser(line: String) = {

    line match {
      case myregex(order_id, date, customer_id, order_status) =>
        Orders(order_id.toInt, customer_id.toInt, order_status)
    }
  }

  val lines = spark.sparkContext.textFile("C:/Users/gotur/Documents/SparkDataSets/Week12/orders_new.csv")

  import spark.implicits._

  val ordersDS = lines.map(parser).toDS().cache()

  ordersDS.printSchema()

  ordersDS.select("order_id").show()

  ordersDS.groupBy("order_status").count().show()

  ordersDS.where(" order_status= 'PENDING_PAYMENT'").show()

  ordersDS.show

  lines.take(10).foreach(println)

  spark.stop()

}