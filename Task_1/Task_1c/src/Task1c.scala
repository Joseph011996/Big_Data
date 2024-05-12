import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {
    // Load each line of the input data
    val bankdataLines = sc.textFile("Assignment_Data/bank-small.csv")
    // Split each line of the input data into an array of strings
    val bankdata = bankdataLines.map(_.split(";"))

    // TODO: *** Put your solution here ***
    //takes the Balance data, converts to Int type and compare with the values
    //Finally makes tuples with the groups and use reduceByKey to find the count for
    //each category
    val groupBalance = bankdata.map(t => {
    	if (t(5).toInt <= 500){
    		("Low",1)
    	}else if (t(5).toInt >= 1501){
    		("High",1)
    	}else{
    		("Medium",1)}
    	})
    	
    //groupBalance.foreach(println)
    val groupBalanceCount = groupBalance.reduceByKey(_+_)
    		
    //groupBalanceCount.collect
    
    groupBalanceCount.saveAsTextFile(
    "file:///root/labfiles/BDC/Task_1/Task_1c-out")

  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1c")
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
