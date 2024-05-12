import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("Assignment_Data/vocab-small.txt").
      as[VocabWord]
    val frequentDocwordsFilename = "Assignment_Data/frequent_docwords.parquet"

    // TODO: *** Put your solution here ***
    val freDocWords = spark.read.parquet(frequentDocwordsFilename)
    
    val wordsJoined = freDocWords.join(vocab, "vocabId")
    
    val wordsSelfJoined = wordsJoined.as("w1").join(wordsJoined.as("w2"), 
    	col("w1.docId") === col("w2.docId") && col("w1.vocabId") > col("w2.vocabId"))
    	
    val resultGrouped = wordsSelfJoined.groupBy($"w1.word"as "first_word", $"w2.word" as 		"second_word").count.orderBy($"count".desc)
    
//-----------------------------------------------------------------    
    resultGrouped.write.mode("overwrite").
	csv("file:///root/labfiles/BDC/Task_4/Task_4b-out.csv")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task4b")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
