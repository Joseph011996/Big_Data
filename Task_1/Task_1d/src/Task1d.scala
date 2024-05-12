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
    //Each person whose job category has an average balance above 500: 
    //education, balance, job, marital, loan.
    
    val job =  bankdata.map(t => (t(1), t(5).toDouble))
    val totalJob = job.groupByKey.mapValues(x => x.sum/x.size).filter(_._2 > 500)
   
    //Array(blue-collar, services, management)
        
    val jobAverage =  bankdata.map(r => (r(1),(r(3), r(5).toDouble, r(1), r(2), r(7))))
    //Array((tertiary,2143.0,management,married,yes), (secondary,29.0,technician,divorced,yes), 		
    //(secondary,2.0,entrepreneur,single,no))
    
    
    val resultJob = totalJob.join(jobAverage).map(j => j._2._2).sortBy(s => s._2, false)
    

    resultJob.coalesce(1).saveAsTextFile(
    "file:///root/labfiles/BDC/Task_1/Task_1d-out")
    }
    


  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1d")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}

