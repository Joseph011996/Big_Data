import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val docwords = spark.read.
      schema(Encoders.product[Docword].schema).
      option("delimiter", " ").
      csv("Assignment_Data/docword-small.txt").
      as[Docword]
    val frequentDocwordsFilename = "Assignment_Data/frequent_docwords.parquet"

    // TODO: *** Put your solution here ***
    
    val commonWords = docwords.groupBy($"vocabId").
    	agg(sum("count").alias("count")).
    	select($"vocabId").
    	where ($"count" >= 1000)
    	
    //commonWords.show
    
    val resultWords = docwords.join(commonWords, "vocabId").
    	select($"vocabId",$"docId",$"count")
    	
    //csv file save directory
    resultWords.write.mode("overwrite").
	csv("file:///root/labfiles/BDC/Task_4/Task_4a-out.csv")
	
    resultWords.write.mode("overwrite").
	parquet(frequentDocwordsFilename)

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task4a")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
