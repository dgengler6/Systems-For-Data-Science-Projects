package predict

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  println("Loading training data from: " + conf.train()) 
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test()) 
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(test.count == 20000, "Invalid test data")

  // CODE
  def computeGlobalMAE(): Double ={
    val globalPred = train.map(r => r.rating).mean()
    val globalMae = test.map(r => scala.math.abs(r.rating - globalPred)).reduce(_+_) / test.count.toDouble
    //println(s"Found global pred $globalPred giving global Mae $globalMae")
    globalMae
  }

  // Per User Prediction
  def computePerUserMAE(): Double = {
    val perUserPred = train.groupBy(r => r.user).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap
    val perUserMae = test.map(r => scala.math.abs(r.rating - perUserPred.getOrElse(r.user,0.0))).reduce(_+_) / test.count.toDouble
    //println(s"Found per User Mae $perUserMae")
    perUserMae
  }

  // Per Item Prediction
  def computePerItemMAE(): Double = {
    // We re compute the globalPrediction to replace unrated items
    val globalPred = train.map(r => r.rating).mean()

    val perItemPred = train.groupBy(r => r.item).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap
    val perItemMae = test.map(r => scala.math.abs(r.rating - perItemPred.getOrElse(r.item, globalPred))).reduce(_+_) / test.count.toDouble
    //println(s"Found per item Mae $perItemMae")
    perItemMae
  }

  // Helper Functions
  def scale(x : Double, r : Double): Double = (x,r) match{
    case (xs,rs) if xs > rs => 5 - rs
    case (xs,rs) if xs < rs => rs - 1
    case _ => 1
  }
  def normalizedDeviation(currentValue : Double, userAverage: Double): Double = (currentValue - userAverage) / scale(currentValue, userAverage)
  def predictUserItem(userID : Int, itemID: Int, userPred: Map[Int, Double], globalAverageDeviation: Map[Int, Double]) : Double ={
    val avgUser = userPred.getOrElse(userID, 0.0)
    val gad = globalAverageDeviation.getOrElse(itemID,0.0)
    avgUser + gad * scale(avgUser + gad, avgUser)
  }

  def computeBaselineMAE(): Double = {
    val perUserPred = train.groupBy(r => r.user).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap

    val globalAverageDeviation = train.groupBy(r => r.item).map(u => (u._1, u._2.map(r=>normalizedDeviation(r.rating, perUserPred.getOrElse(r.user, 0.0))).reduce(_+_) / u._2.size)).collect.toMap
    val baselineMethodMae = test.map(r => scala.math.abs(r.rating - predictUserItem(r.user, r.item, perUserPred, globalAverageDeviation))).reduce(_+_) / test.count.toDouble
    //println(s"Found Baseline Mae $baselineMethodMae")
    baselineMethodMae
  }


  def benchmarkFunction(f: () => Double): (Double, Double, Double, Double) = {

    // Runs the given function 10 times and keeps track of the duration
    val iterations = for {
      n <- 1 to 10
      startTime = System.currentTimeMillis()
      result = f()
      endTime = System.currentTimeMillis()
      duration = endTime - startTime
    } yield (duration)

    val min = iterations.min
    val max = iterations.max
    val average = iterations.sum / iterations.size
    val std = scala.math.sqrt( iterations.map(x => scala.math.pow(x - average, 2)).sum / iterations.size )
    (min, max, average, std)
  }

  val (minGlobal, maxGlobal, averageGlobal, stdGlobal) = benchmarkFunction(computeGlobalMAE)
  val (minPerUser, maxPerUser, averagePerUser, stdPerUser) = benchmarkFunction(computePerUserMAE)
  val (minPerItem, maxPerItem, averagePerItem, stdPerItem) = benchmarkFunction(computePerItemMAE)
  val (minBaseline, maxBaseline, averageBaseline, stdBaseline) = benchmarkFunction(computeBaselineMAE)
  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
            "Q3.1.4" -> Map(
              "MaeGlobalMethod" -> computeGlobalMAE(), // Datatype of answer: Double
              "MaePerUserMethod" -> computePerUserMAE(), // Datatype of answer: Double
              "MaePerItemMethod" -> computePerItemMAE(), // Datatype of answer: Double
              "MaeBaselineMethod" -> computeBaselineMAE() // Datatype of answer: Double
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> minGlobal,  // Datatype of answer: Double
                "max" -> maxGlobal,  // Datatype of answer: Double
                "average" -> averageGlobal, // Datatype of answer: Double
                "stddev" -> stdGlobal // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> minPerUser,  // Datatype of answer: Double
                "max" -> maxPerUser,  // Datatype of answer: Double
                "average" -> averagePerUser, // Datatype of answer: Double
                "stddev" -> stdPerUser // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> minPerItem,  // Datatype of answer: Double
                "max" -> maxPerItem,  // Datatype of answer: Double
                "average" -> averagePerItem, // Datatype of answer: Double
                "stddev" -> stdPerItem // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> minBaseline,  // Datatype of answer: Double
                "max" -> maxBaseline, // Datatype of answer: Double
                "average" -> averageBaseline, // Datatype of answer: Double
                "stddev" -> stdBaseline // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> averageBaseline / averageGlobal // Datatype of answer: Double
            ),
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
