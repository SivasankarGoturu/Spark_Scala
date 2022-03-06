import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger



object AgeCalculation extends App{
  
  def parseInput(line: String) = {
    var name = line.split(",")(0)
    var age = line.split(",")(1).toInt
    var city = line.split(",")(2)

    (name, age, city, if (age > 18) "Y" else "N")

  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "friendsdata")

  val input = sc.textFile("C:/Users/gotur/Documents/SparkDataSets/Assigniment/Age")
  val reqInput = input.map(parseInput)

  reqInput.collect.foreach(println)

  
  
}