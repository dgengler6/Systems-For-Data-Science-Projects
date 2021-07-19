package similarity

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

  // Helper Functions
  def scale(x : Double, r : Double): Double = (x,r) match{
    case (xs,rs) if xs > rs => 5 - rs
    case (xs,rs) if xs < rs => rs - 1
    case _ => 1
  }

  def normalizedDeviation(currentValue : Double, userAverage: Double): Double = (currentValue - userAverage) / scale(currentValue, userAverage)

  // COSINE SIMILARITY

  def cosineSimilarityOpti(u : Int, v : Int, itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]]) : Double = {

    val ratedByU = itemsRatedByUsers.getOrElse(u, Set.empty[Int])
    val ratedByV = itemsRatedByUsers.getOrElse(v, Set.empty[Int])
    val interUV = ratedByU.intersect(ratedByV).toList

    val ratingsByU = itemsRatingsByUsers.getOrElse(u, Iterable.empty[(Int, (Double, Double))]).toMap
    val ratingsByV = itemsRatingsByUsers.getOrElse(v, Iterable.empty[(Int, (Double, Double))]).toMap

    if(ratedByU.union(ratedByV).isEmpty )//|| ratingsByU.values.sum == 0 || ratingsByV.values.sum == 0)
    {
      0.0
    }
    else
    {
      interUV.map(i => ratingsByU.getOrElse(i, (0.0, 0.0))._2 * ratingsByV.getOrElse(i, (0.0, 0.0))._2).sum
    }

  }

  def userSpecificWeightedSumDeviationOpti(userID : Int, itemID: Int, utri : Map[Int, Iterable[Int]], itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]]) : Double ={
    //val usersThatRatedI = train.groupBy(_.user).map(u => (u._1, u._2.map(r => (r.item, r.rating)).toMap)).filter(_._2.keys.toSet.contains(itemID))
    val cosine = utri.getOrElse(itemID, Iterable.empty[Int]).filter(_ != userID).map(u => {val sim = userSimilarities.getOrElse((userID, u), 0.0) ; (scala.math.abs(sim), sim * itemsRatingsByUsers.getOrElse(u, Iterable.empty[(Int, (Double, Double))]).toMap.getOrElse(itemID, (0.0, 0.0))._1)})
    val denom = cosine.map(_._1).sum
    if (denom == 0){
      0.0
    }else{
      cosine.map(_._2).sum / denom
    }
  }

  def predictUserItemCosineOpti(userID : Int, itemID: Int, userPred: Map[Int, Double], utri : Map[Int, Iterable[Int]], itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]]) : Double ={
    val avgUser = userPred.getOrElse(userID, 0.0)

    val uswsd = userSpecificWeightedSumDeviationOpti(userID, itemID, utri, itemsRatedByUsers, itemsRatingsByUsers)
    val pred = avgUser + uswsd * scale(avgUser + uswsd, avgUser)
    //println(s"User : $userID, Item : $itemID => Pred : $pred")
    pred
  }

  // PRECOMPUTE SIMILARITIES
  println("Precompute Maps")

  // Compute the per user predictions
  val perUserPred = train.groupBy(r => r.user).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap

  // Compute the map of the set of user that rated a give item
  val utri = train.groupBy(_.item).mapValues(_.map(_.user)).collect.toMap

  // For each user return the map point user to the set of item they rated
  val itemsRatedByUsers = train.groupBy(_.user).map(u => (u._1, u._2.map(_.item).toSet)).collect.toMap

  // For each user return the list of rated items and corresponding normalized deviation
  val vbu = train.groupBy(_.user)
    .mapValues(r => r.map(v => (v.item, normalizedDeviation(v.rating, perUserPred.getOrElse(v.user, 0.0))))) //.collect.toMap

  // For each user returns the normalizing factor
  val norm = train.groupBy(_.user).mapValues(v => v.map(r => normalizedDeviation(r.rating, perUserPred.getOrElse(r.user, 0.0)))).mapValues(v => math.sqrt(v.map(math.pow(_, 2)).sum))

  // Returns the rating and r normalized for each user and item
  val itemsRatingsByUsers = vbu.join(norm).mapValues(v => v._1.map(u => (u._1, (u._2, u._2/v._2)))).collect.toMap

  /*
  I tried to implement using cartesian products on RDDs but I really took forever ...
  val users = train.groupBy(_.user).map(_._1)
  val userSimilarities = users.cartesian(users).filter(up => up._1 != up._2)
    .map{case(u1, u2) => ((u1 , u2), cosineSimilarityOpti(u1 : Int, u2 : Int, itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]]))}.collectAsMap()
  */

  val users = train.groupBy(_.user).map(_._1).collect

  // Compute the similarity for each user
  def computeUserSimilarities(): Map[(Int, Int), Double] = {
    (for{
      u1 <- users
      u2 <- users
      if u1 != u2
    } yield ((u1 , u2), cosineSimilarityOpti(u1 : Int, u2 : Int, itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]]))).toMap
  }
  val userSimilarities = computeUserSimilarities()
  println("Done precomputing")

  def cosineBasedMAEOpti(): Double = {
    println("Computing MAE using Cosine Similarity preprocessed")
    test.map(r => scala.math.abs(r.rating - predictUserItemCosineOpti(r.user, r.item, perUserPred, utri, itemsRatedByUsers, itemsRatingsByUsers))).reduce(_+_) / test.count.toDouble
  }

  val baselineMAE = 0.7669
  val cosineMAE = cosineBasedMAEOpti()

  // JACCARD SIMILARITY
  def jaccard(a: Set[Int], b: Set[Int]): Double = {
    // If both sets are empty return 0.0 as similarity to avoid division by zero
    if (a.isEmpty && b.isEmpty) {
      0.0
    } else {
      val inter = (a & b).size.toDouble
      inter / (a.size.toDouble + b.size.toDouble - inter)
    }
  }

  def jaccardSimilarity(u : Int, v : Int, itemsRatedByUsers : Map[Int, Set[Int]]) : Double = {
    val ratedByU = itemsRatedByUsers.getOrElse(u, Set.empty[Int])
    val ratedByV = itemsRatedByUsers.getOrElse(v, Set.empty[Int])

    jaccard(ratedByU, ratedByV)
  }

  def userSpecificWeightedSumDeviationJaccard(userID : Int, itemID: Int, utri : Map[Int, Iterable[Int]], itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, Double)]]) : Double ={
    //val usersThatRatedI = train.groupBy(_.user).map(u => (u._1, u._2.map(r => (r.item, r.rating)).toMap)).filter(_._2.keys.toSet.contains(itemID))
    val cosine = utri.getOrElse(itemID, Iterable.empty[Int]).filter(_ != userID).map(u => {val sim = jaccardSimilarity(userID, u, itemsRatedByUsers) ; (scala.math.abs(sim), sim * itemsRatingsByUsers.getOrElse(u, Iterable.empty[(Int, Double)]).toMap.getOrElse(itemID, 0.0))})
    val denom = cosine.map(_._1).sum
    if (denom == 0){
      0.0
    }else{
      cosine.map(_._2).sum / denom
    }
  }

  def predictUserItemJaccard(userID : Int, itemID: Int, userPred: Map[Int, Double], utri : Map[Int, Iterable[Int]], itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, Double)]]) : Double ={
    val avgUser = userPred.getOrElse(userID, 0.0)

    val uswsd = userSpecificWeightedSumDeviationJaccard(userID, itemID, utri, itemsRatedByUsers, itemsRatingsByUsers)
    avgUser + uswsd * scale(avgUser + uswsd, avgUser)
  }

  def jaccardBasedMAE(): Double = {
    println("Computing MAE using Jaccard Similarity")
    val perUserPred = train.groupBy(r => r.user).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap
    val utri = train.groupBy(_.item).mapValues(_.map(_.user)).collect.toMap

    val itemsRatedByUsers = train.groupBy(_.user).map(u => (u._1, u._2.map(_.item).toSet)).collect.toMap

    val itemsRatingsByUsers = train.groupBy(_.user)
      .mapValues(r => r.map(v => (v.item, normalizedDeviation(v.rating, perUserPred.getOrElse(v.user, 0.0))))).collect.toMap

    test.map(r => scala.math.abs(r.rating - predictUserItemJaccard(r.user, r.item, perUserPred, utri, itemsRatedByUsers, itemsRatingsByUsers))).reduce(_+_) / test.count.toDouble
  }

  val jaccardMAE = jaccardBasedMAE()


  // Worst Case number of similarities to compute
  val nbSimU1 = users.size * (users.size - 1)

  // Number of Multiplications required
  def cosineSimStats() : (Double, Double, Double, Double) = {
    val nbMultiplications = for{
      u1 <- users
      u2 <- users
      ratedByU = itemsRatedByUsers.getOrElse(u1, Set.empty[Int])
      ratedByV = itemsRatedByUsers.getOrElse(u2, Set.empty[Int])
      if u1 != u2
    } yield (ratedByU.intersect(ratedByV).size)

    val minMultiplications = nbMultiplications.min
    val maxMultiplications = nbMultiplications.max
    val avgMultiplications = nbMultiplications.sum / nbMultiplications.size
    val stdMultiplications = scala.math.sqrt( nbMultiplications.map(x => scala.math.pow(x - avgPredictions, 2)).sum / nbMultiplications.size )


    (minMultiplications, maxMultiplications, avgMultiplications, stdMultiplications)
  }

  val (minMultiplications, maxMultiplications, avgMultiplications, stdMultiplications) = cosineSimStats()

  // Memory requires for storage

  val totalBytesSimilarities = userSimilarities.values.filter(_ != 0.0).size * 8

  // Benchmarking

  def benchmarkCosineMAE(): (Double, Double, Double, Double, Double, Double, Double, Double, Double, Double) = {
    println("Running Benchmark for CosineMAE")
    // Runs the given function 5 times and keeps track of the duration
    val iterations = for {
      n <- 1 to 5
      // Benchmark for Cosine based MAE
      t1 = System.currentTimeMillis()
      userSimilarities = computeUserSimilarities()
      t2 = System.currentTimeMillis()
      similaritiesDuration = t2 - t1
      // Benchmark for Computing Similarities
      result = cosineBasedMAEOpti()
      t3 = System.currentTimeMillis()
      similarities = userSimilarities.size
      predictionDuration = t3 - t2
    } yield (predictionDuration + similaritiesDuration, similarities, similaritiesDuration)

    val predDurations = iterations.map(_._1)
    val simDurations = iterations.map(_._3)

    // Compute statistics about predictions
    val minPredictions = predDurations.min
    val maxPredictions = predDurations.max
    val avgPredictions = predDurations.sum / predDurations.size.toDouble
    val stdPredictions = scala.math.sqrt( predDurations.map(x => scala.math.pow(x - avgPredictions, 2)).sum / predDurations.size )

    // Compute statistics about similarities
    val minSimilarities = simDurations.min
    val maxSimilarities = simDurations.max
    val avgSimilarities = simDurations.sum / simDurations.size.toDouble
    val stdSimilarities = scala.math.sqrt( simDurations.map(x => scala.math.pow(x - avgSimilarities, 2)).sum / simDurations.size )

    val timePerSimilarity = iterations.map{ case (_, nbSimComputed, totalSimilarityDuration) => totalSimilarityDuration / nbSimComputed.toDouble}
    val avgTimePerSimilarity = timePerSimilarity.sum / timePerSimilarity.size
    val ratioSimilarityOverPredictions = avgSimilarities.toDouble / avgPredictions.toDouble

    (minPredictions, maxPredictions, avgPredictions, stdPredictions, minSimilarities, maxSimilarities, avgSimilarities, stdSimilarities, avgTimePerSimilarity, ratioSimilarityOverPredictions )
  }

  val (minPredictions, maxPredictions, avgPredictions, stdPredictions, minSimilarities, maxSimilarities, avgSimilarities, stdSimilarities, avgTimePerSimilarity, ratioSimilarityOverPredictions )  = benchmarkCosineMAE()


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
          "Q2.3.1" -> Map(
            "CosineBasedMae" -> cosineMAE, // Datatype of answer: Double
            "CosineMinusBaselineDifference" -> (cosineMAE - baselineMAE) // Datatype of answer: Double
          ),

          "Q2.3.2" -> Map(
            "JaccardMae" -> jaccardMAE, // Datatype of answer: Double
            "JaccardMinusCosineDifference" -> (jaccardMAE - cosineMAE) // Datatype of answer: Double
          ),

          "Q2.3.3" -> Map(
            // Provide the formula that computes the number of similarity computations
            // as a function of U in the report.
            "NumberOfSimilarityComputationsForU1BaseDataset" -> nbSimU1 // Datatype of answer: Int
          ),

          "Q2.3.4" -> Map(
            "CosineSimilarityStatistics" -> Map(
              "min" -> minMultiplications,  // Datatype of answer: Double
              "max" -> maxMultiplications, // Datatype of answer: Double
              "average" -> avgMultiplications, // Datatype of answer: Double
              "stddev" -> stdMultiplications // Datatype of answer: Double
            )
          ),

          "Q2.3.5" -> Map(
            // Provide the formula that computes the amount of memory for storing all S(u,v)
            // as a function of U in the report.
            "TotalBytesToStoreNonZeroSimilarityComputationsForU1BaseDataset" -> totalBytesSimilarities // Datatype of answer: Int
          ),

          "Q2.3.6" -> Map(
            "DurationInMicrosecForComputingPredictions" -> Map(
              "min" -> minPredictions,  // Datatype of answer: Double
              "max" -> maxPredictions, // Datatype of answer: Double
              "average" -> avgPredictions, // Datatype of answer: Double
              "stddev" -> stdPredictions // Datatype of answer: Double
            )
            // Discuss about the time difference between the similarity method and the methods
            // from milestone 1 in the report.
          ),

          "Q2.3.7" -> Map(
            "DurationInMicrosecForComputingSimilarities" -> Map(
              "min" -> minSimilarities,  // Datatype of answer: Double
              "max" -> maxSimilarities, // Datatype of answer: Double
              "average" -> avgSimilarities, // Datatype of answer: Double
              "stddev" -> stdSimilarities // Datatype of answer: Double
            ),
            "AverageTimeInMicrosecPerSuv" -> avgTimePerSimilarity, // Datatype of answer: Double
            "RatioBetweenTimeToComputeSimilarityOverTimeToPredict" -> ratioSimilarityOverPredictions // Datatype of answer: Double
          )
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
