package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import predict.Predictor.normalizedDeviation
import predict.Predictor.predictUserItem
import predict.Predictor.scale
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Recommender extends App {
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
  println("Loading data from: " + conf.data()) 
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(data.count == 100000, "Invalid data")

  println("Loading personal data from: " + conf.personal()) 
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles

  // Extract the movies we rated (ID : 944)
  val personalRatings = personalFile.map(l => l.split(",").map(_.trim))
    .filter(cols => cols.size == 3)
    .map(cols => Rating(944, cols(0).toInt, cols(2).toDouble))


  // extract all titles
  //val movieTitles = personalFile.map(l => { val cols = l.split(",").map(_.trim); (cols(0).toInt, cols(1)) })

  val unratedMovies = personalFile.map(l => l.split(",").map(_.trim))
    .filter(cols => cols.size == 2)
    .map(cols => (cols(0).toInt, cols(1)))

  assert(personalFile.count == 1682, "Invalid personal data")

  // Add our ratings to the database
  val allData = data.union(personalRatings)


  def predictTopkMovies(userId : Int, k: Int) : List[List[Any]] ={
    val perUserPred = allData.groupBy(r => r.user).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap
    val perItemQueries = allData.groupBy(r => r.item).map(r => (r._1, r._2.size.toDouble)).collect.toMap
    val globalAverageDeviation = allData.groupBy(r => r.item).map(u => (u._1, u._2.map(r=>normalizedDeviation(r.rating, perUserPred.getOrElse(r.user, 0.0))).reduce(_+_) / u._2.size)).collect.toMap

    val predictedItems = unratedMovies.map(r => (r._1, r._2, predictUserItem(userId, r._1, perUserPred, globalAverageDeviation))).sortBy(_._3, ascending = false).map(t => List(t._1, t._2, t._3))
    //println(s"The top $k movie recommended for user $userId using the original Baseline prediction algorithm.")
    //for (n <- predictedItems.take(k)) println(n)
    return predictedItems.take(k).toList

  }

  // BONUS

  def scaledSigmoid(x : Double): Double = {
    val b = -300
    val a = 0.023
    1 / (1 + scala.math.exp(-a * (x + b)))
  }

  def bonusPredictUserItem(userID : Int, itemID: Int, userPred: Map[Int, Double], perItemQueries: Map[Int, Double], globalAverageDeviation: Map[Int, Double]) : Double ={
    val avgUser = userPred.getOrElse(userID, 0.0)
    val gad = globalAverageDeviation.getOrElse(itemID,0.0)
    val nbQueries = perItemQueries.getOrElse(itemID,0.0)
    avgUser + gad * scale(avgUser + gad, avgUser) * scaledSigmoid(nbQueries)
  }

  def bonusPredictTopkMovies(userId : Int, k: Int) : List[List[Any]] ={
    val perUserPred = allData.groupBy(r => r.user).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap
    val perItemQueries = allData.groupBy(r => r.item).map(r => (r._1, r._2.size.toDouble)).collect.toMap
    val globalAverageDeviation = allData.groupBy(r => r.item).map(u => (u._1, u._2.map(r=>normalizedDeviation(r.rating, perUserPred.getOrElse(r.user, 0.0))).reduce(_+_) / u._2.size)).collect.toMap

    val predictedItems = unratedMovies.map(r => (r._1, r._2, bonusPredictUserItem(userId, r._1, perUserPred, perItemQueries, globalAverageDeviation))).sortBy(_._3, ascending = false).map(t => List(t._1, t._2, t._3))

    println(s"BONUS : The top $k movie recommended for user $userId using the modified Baseline prediction algorithm.")
    for (n <- predictedItems.take(k)) println(n)
    return predictedItems.take(k).toList

  }
  for w1 in range(-5,5,0.1):
    for w2 in range(-5,5,0.1):
      for b1 in range(-5,5,0.1):
        for b2 in range(-5,5,0.1):
  bonusPredictTopkMovies(944, 5)

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

            // IMPORTANT: To break ties and ensure reproducibility of results,
            // please report the top-5 recommendations that have the smallest
            // movie identifier.

            "Q4.1.1" -> predictTopkMovies(944, 5)// Datatypes for answer: Int, String, Double
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
