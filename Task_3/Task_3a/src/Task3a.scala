import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val docwords = spark.read.
      schema(Encoders.product[Docword].schema).
      option("delimiter", " ").
      csv("Assignment_Data/docword-small.txt").
      as[Docword]
    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("Assignment_Data/vocab-small.txt").
      as[VocabWord]

    // TODO: *** Put your solution here ***
    // Cal Total Count each word across in all docs
    // words in asc order
    
    //case class countDF (word: String, sum_count: Int)
    
    val totalCount = docwords.select($"vocabId",$"count").
	join(vocab, "vocabId").
	groupBy($"word").
	sum("count").
	orderBy($"word")
	
    //val countOrdered = totalCount.orderBY($"word".desc).show(10)

    //csv file save directory
    totalCount.write.mode("overwrite").
	csv("file:///root/labfiles/BDC/Task_3/Task_3a-out.csv")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3a")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
