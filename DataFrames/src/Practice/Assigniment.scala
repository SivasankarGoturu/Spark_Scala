package Practice

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Assigniment extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf
  conf.set("spark.master", "local[*]")
  conf.set("spark.app.name", "crecket")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  val venueDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/ipl_dataset/ipl_venue.csv")
    .load

    import spark.implicits._
    
  val matchesDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/ipl_dataset/ipl_matches.csv")
    .load

  val ballDf = spark.read
    .format("csv")
    .option("mode", "PERMISSIVE")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/DatasetToCompleteTheSixSparkExercises/ipl_dataset/ipl_ball_by_ball.csv")
    .load
    
    val joinType = "inner"
    val joinCondition = matchesDf.col("match_id") === ballDf.col("match_id")
    val joinCondition2 = venueDf.col("venue_id") === matchesDf.col("venue_id")
    
    
    
    val newDf = matchesDf.join(ballDf, joinCondition, joinType).join(venueDf, joinCondition2, joinType)
    
    newDf.show
    
    newDf.select(matchesDf.col("match_id"), ballDf.col("match_id")).show
    
    
//
//  // ballDf.show()
//  //matchesDf.show()
//  //venueDf.show()
//
//  venueDf.createOrReplaceTempView("venue")
//  //|venue_id|               venue|          city|
//  matchesDf.createOrReplaceTempView("matches")
//  //|match_id|               date|player_of_match|venue_id|neutral_venue|               team1|               team2|
//  //toss_winner|toss_decision|              winner| result|result_margin|eliminator|method|    umpire1|       umpire2|
//  ballDf.createOrReplaceTempView("balls")
//  //|match_id|inning|overs|ball|      batsman|  non_striker|      bowler|batsman_runs|extra_runs|total_runs|
//  //non_boundary|is_wicket|dismissal_kind|player_dismissed|fielder|extras_type|        batting_team|        bowling_team|
//
////  A. Find the top 3 venues which hosted the most number of eliminator matches?
//  
//  spark.sql("""
//  select v.venue,count(m.eliminator) NoOfEleminations from venue v
//  left join matches m on m.venue_id = v.venue_id
//  left join balls b on b.match_id = m.match_id
//  where m.eliminator = 'Y'
//  group by v.venue
//  order by NoOfEleminations desc
//  limit 3
//  """).show
//  
////  B. Return the most number of catches taken by a player in IPL history?
//
//   spark.sql("""
//  select fielder,count(dismissal_kind) NoOfcatches from balls
//  where dismissal_kind = 'caught'
//  group by fielder
//  order by NoOfcatches desc
//  limit 1
//  """).show
//  
//  
//
//  //spark.sql("""select distinct(eliminator) from matches""").show
//  //spark.sql("""select * from balls""").show
//
////  Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
////+--------------------+----------------+
////|               venue|NoOfEleminations|
////+--------------------+----------------+
////|Dubai Internation...|             743|
////|Sheikh Zayed Stadium|             494|
////|            Newlands|             256|
////+--------------------+----------------+
//  
}