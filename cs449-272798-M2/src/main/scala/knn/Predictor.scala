package knn

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

  def scale(x : Double, r : Double): Double = (x,r) match{
    case (xs,rs) if xs > rs => 5 - rs
    case (xs,rs) if xs < rs => rs - 1
    case _ => 1
  }

  def normalizedDeviation(currentValue : Double, userAverage: Double): Double = (currentValue - userAverage) / scale(currentValue, userAverage)

  // PRECOMPUTE SIMILARITIES
  println("Precompute Maps")
  val perUserPred = train.groupBy(r => r.user).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap
  val utri = train.groupBy(_.item).mapValues(_.map(_.user)).collect.toMap

  val itemsRatedByUsers = train.groupBy(_.user).map(u => (u._1, u._2.map(_.item).toSet)).collect.toMap
  //val itemsRatingsByUsers
  val vbu = train.groupBy(_.user)
    .mapValues(r => r.map(v => (v.item, normalizedDeviation(v.rating, perUserPred.getOrElse(v.user, 0.0))))) //.collect.toMap
  val norm = train.groupBy(_.user).mapValues(v => v.map(r => normalizedDeviation(r.rating, perUserPred.getOrElse(r.user, 0.0)))).mapValues(v => math.sqrt(v.map(math.pow(_, 2)).sum))
  val itemsRatingsByUsers = vbu.join(norm).mapValues(v => v._1.map(u => (u._1, (u._2, u._2/v._2)))).collect.toMap
  val users = train.groupBy(_.user).map(_._1).collect

  def computeUserSimilarities(): Array[((Int, Int), Double)] = {
    (for{
      u1 <- users
      u2 <- users
      if u1 != u2
    } yield ((u1 , u2), cosineSimilarityOpti(u1 : Int, u2 : Int, itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]])))
  }

  val userSimilarities = computeUserSimilarities().groupBy(_._1._1).mapValues(_.toSeq.sortBy{ case (_, sim) => sim }(Ordering[Double].reverse))

  println("Done precomputing")


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

  def userSpecificWeightedSumDeviationOpti(userID : Int, itemID: Int, utri : Map[Int, Iterable[Int]], itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]],sims : Map[(Int, Int), Double],  k : Int = 10) : Double ={
    val cosine = utri.getOrElse(itemID, Iterable.empty[Int]).filter(_ != userID).map(u => {val sim = sims.getOrElse((userID, u), 0.0) ; (scala.math.abs(sim), sim * itemsRatingsByUsers.getOrElse(u, Iterable.empty[(Int, (Double, Double))]).toMap.getOrElse(itemID, (0.0, 0.0))._1)})
    //val cosine = utri.getOrElse(itemID, Iterable.empty[Int]).filter(_ != userID).map(u => (u, userSimilarities.getOrElse((userID, u), 0.0))).toSeq
    //  .sortBy{ case (_, sim) => sim }(Ordering[Double].reverse).take(k).map{case (user, sim) => (scala.math.abs(sim), sim * itemsRatingsByUsers.getOrElse(user, Iterable.empty[(Int, (Double, Double))]).toMap.getOrElse(itemID, (0.0, 0.0))._1)}
    val denom = cosine.map(_._1).sum
    if (denom == 0){
      0.0
    }else{
      cosine.map(_._2).sum / denom
    }
  }

  def predictUserItemCosineOpti(userID : Int, itemID: Int, userPred: Map[Int, Double], utri : Map[Int, Iterable[Int]], itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]], sims : Map[(Int, Int), Double]) : Double ={
    val avgUser = userPred.getOrElse(userID, 0.0)

    val uswsd = userSpecificWeightedSumDeviationOpti(userID, itemID, utri, itemsRatedByUsers, itemsRatingsByUsers, sims)
    val pred = avgUser + uswsd * scale(avgUser + uswsd, avgUser)
    //println(s"User : $userID, Item : $itemID => Pred : $pred")
    pred
  }

  def cosineBasedMAEOpti(k : Int = 10): Double = {
    println(s"Computing MAE using Cosine Similarity for k = $k")
    //val sims = userSimilarities.toSeq.sortBy{ case (_, sim) => sim }(Ordering[Double].reverse).take(k).toMap
    val sims = userSimilarities.mapValues(_.take(k).toMap).map(identity)

    println("Done with sims")
    test.map(r => scala.math.abs(r.rating - predictUserItemCosineOpti(r.user, r.item, perUserPred, utri, itemsRatedByUsers, itemsRatingsByUsers, sims.getOrElse(r.user, Map.empty[(Int, Int), Double])))).reduce(_+_) / test.count.toDouble
  }
  val nbUsers = train.groupBy(_.user).map(_._1).collect.size
  def numberOfBytesForK(k : Int) : Int = {
    nbUsers * k * 8
  }

  val baselineMAE = 0.7669

  def cosineUsingKNNMAE() : (Double, Double, Double, Double, Double, Double, Double, Double, Double, Int,  Double) ={
    val ks = List(10, 30, 50, 100, 200, 300, 400, 800, 943)

    val kmaes = ks.map(k => (k, cosineBasedMAEOpti(k)))

    val (lowestKbetterThanMAE, lowestKMAE) = kmaes.filter(_._2 < baselineMAE).min

    val lowestKMAEminusBaseline = lowestKMAE - baselineMAE

    kmaes.map(_._2) match {
      case List(k10, k30, k50, k100, k200, k300, k400, k800, k943) => (k10, k30, k50, k100, k200, k300, k400, k800, k943, lowestKbetterThanMAE, lowestKMAEminusBaseline)
    }

  }

  val (k10, k30, k50, k100, k200, k300, k400, k800, k943, lowestKbetterThanMAE, lowestKMAEminusBaseline) = cosineUsingKNNMAE()


  val sizeOfRam = 8000000000L

  def nbUsersGivenLowestK(k : Int) : Long = {
    sizeOfRam / (nbUsers * k * 3 * 8).toLong
  }
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
          "Q3.2.1" -> Map(
            // Discuss the impact of varying k on prediction accuracy on
            // the report.
            "MaeForK=10" -> k10, // Datatype of answer: Double
            "MaeForK=30" -> k30, // Datatype of answer: Double
            "MaeForK=50" -> k50, // Datatype of answer: Double
            "MaeForK=100" -> k100, // Datatype of answer: Double
            "MaeForK=200" -> k200, // Datatype of answer: Double
            "MaeForK=300" -> k300, // Datatype of answer: Double
            "MaeForK=400" -> k400, // Datatype of answer: Double
            "MaeForK=800" -> k800, // Datatype of answer: Double
            "MaeForK=943" -> k943, // Datatype of answer: Double
            "LowestKWithBetterMaeThanBaseline" -> lowestKbetterThanMAE, // Datatype of answer: Int
            "LowestKMaeMinusBaselineMae" -> lowestKMAEminusBaseline // Datatype of answer: Double
          ),

          "Q3.2.2" ->  Map(
            // Provide the formula the computes the minimum number of bytes required,
            // as a function of the size U in the report.
            "MinNumberOfBytesForK=10" -> numberOfBytesForK(10), // Datatype of answer: Int
            "MinNumberOfBytesForK=30" -> numberOfBytesForK(30), // Datatype of answer: Int
            "MinNumberOfBytesForK=50" -> numberOfBytesForK(50), // Datatype of answer: Int
            "MinNumberOfBytesForK=100" -> numberOfBytesForK(100), // Datatype of answer: Int
            "MinNumberOfBytesForK=200" -> numberOfBytesForK(200), // Datatype of answer: Int
            "MinNumberOfBytesForK=300" -> numberOfBytesForK(300), // Datatype of answer: Int
            "MinNumberOfBytesForK=400" -> numberOfBytesForK(400), // Datatype of answer: Int
            "MinNumberOfBytesForK=800" -> numberOfBytesForK(800), // Datatype of answer: Int
            "MinNumberOfBytesForK=943" -> numberOfBytesForK(943) // Datatype of answer: Int
          ),

          "Q3.2.3" -> Map(
            "SizeOfRamInBytes" -> sizeOfRam, // Datatype of answer: Long
            "MaximumNumberOfUsersThatCanFitInRam" -> nbUsersGivenLowestK(lowestKbetterThanMAE) // Datatype of answer: Long
          )

          // Answer the Question 3.2.4 exclusively on the report.
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
