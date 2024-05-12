import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext, x: String, y: String) {
    // Load each line of the input data
    val twitterLines = sc.textFile("Assignment_Data/twitter-small.tsv")
    // Split each line of the input data into an array of strings
    val twitterdata = twitterLines.map(_.split("\t"))

    // TODO: *** Put your solution here ***
    
    val dataRows = twitterdata.map(d => (d(1),d(2)toInt,d(3))).filter(_._2 != "0")
    
    val month_X = dataRows.filter(_._1 == x).map(x => (x._3,x._2))
    val month_Y = dataRows.filter(_._1 == y).map(y => (y._3,y._2))
    
    val monthJoined = month_X.join(month_Y)
    
    monthJoined.cache() // optimized since it'll be used again in multiple queries
    
    val mostCount = monthJoined.mapValues(x => x._2 - x._1)
    
    val hashTag = monthJoined.join(mostCount).sortBy(x => x._2._2, false).first()
    
    println("hashtagName: " + hashTag._1+ 
    	", CountX: "+ hashTag._2._1._1+ 
    	", CountY: "+ hashTag._2._1._2)
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Check command line arguments
    if(args.length != 2) {
      println("Expected two command line arguments: <month x> and <month y>")
    }
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task2c")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext, args(0), args(1))
    // Stop Spark
    spark.stop()
  }
}
