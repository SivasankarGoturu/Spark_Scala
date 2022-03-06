import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import scala.reflect.api.materializeTypeTag

object TopMovies extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.app.name", "solve")
  conf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  case class Movies(movieid: Int, movieName: String, genre: String)
  
  case class Rating(userId: Int, movieId: Int, rating:Int, timeStamp: String)

  def parseLenes(lines: String) = {

    val fields = lines.split("::")

    Movies(fields(0).toInt, fields(1), fields(2))

  }
  
  def parseLenes2(lines: String) ={
    
    val fields = lines.split("::")
    
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3))
    
  }

  import spark.implicits._

  val moviesRdd = spark.sparkContext.textFile("C:/Users/gotur/Documents/SparkDataSets/Week12/Assigniment/movies.dat")

  val requredRdd = moviesRdd.map(parseLenes)

  val newMoviesDf = requredRdd.toDF()
  
  val ratingRdd = spark.sparkContext.textFile("C:/Users/gotur/Documents/SparkDataSets/Week12/Assigniment/ratings.dat")
   
  val requredRdd2 = ratingRdd.map(parseLenes2)
  
  val newRatingDf = requredRdd2.toDF()
  
  newRatingDf.show

//  requredRdd.collect.foreach(println)

}