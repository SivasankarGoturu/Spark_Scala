import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Salting extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext()

  val random = new scala.util.Random

  val start = 10

  val end = 19

  val input = sc.textFile("sparkopt/bigLogNew.txt")

  val rdd1 = input.map(x => {

    var num = start + random.nextInt((end - start) + 1)

    (num + x.split(",")(0), x.split(",")(1))

  })

  val gpval = rdd1.groupByKey()

  val grouping = gpval.map(x => (x._1, x._2.size))

  val output = grouping.map(x => {

    (x._1.substring(2), x._2)

  })

  val fout = output.reduceByKey((x, y) => x + y).sortBy(x => x._2)

  fout.collect.foreach(println)

  
  
}