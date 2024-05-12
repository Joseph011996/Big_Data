import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {
    // Load each line of the input data
    val twitterLines = sc.textFile("Assignment_Data/twitter-small.tsv")
    // Split each line of the input data into an array of strings
    val twitterdata = twitterLines.map(_.split("\t"))

    // TODO: *** Put your solution here ***
    val totalTweets = twitterdata.map(t => (t(3), t(2).toInt))
    .reduceByKey(_ + _)
    .sortBy(_._2,false)
    .first()
    
    println(totalTweets._1 + " " + totalTweets._2)

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task2b")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}
