package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

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


  def scale(x : Double, r : Double): Double = (x,r) match{
    case (xs,rs) if xs > rs => 5 - rs
    case (xs,rs) if xs < rs => rs - 1
    case _ => 1
  }

  def normalizedDeviation(currentValue : Double, userAverage: Double): Double = (currentValue - userAverage) / scale(currentValue, userAverage)

  def cosineSimilarityOpti(u : Int, v : Int, itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]]) : Double = {

    val ratedByU = itemsRatedByUsers.getOrElse(u, Set.empty[Int])
    val ratedByV = itemsRatedByUsers.getOrElse(v, Set.empty[Int])
    val interUV = ratedByU.intersect(ratedByV).toList

    val ratingsByU = itemsRatingsByUsers.getOrElse(u, Iterable.empty[(Int, (Double, Double))]).toMap
    val ratingsByV = itemsRatingsByUsers.getOrElse(v, Iterable.empty[(Int, (Double, Double))]).toMap

    if(ratedByU.union(ratedByV).isEmpty || math.abs(ratingsByU.values.map(_._1).sum) < 0.000001 || math.abs(ratingsByV.values.map(_._1).sum) < 0.000001)
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

  // PRECOMPUTE SIMILARITIES
  println("Precompute Maps")

  val perUserPred = allData.groupBy(r => r.user).map(u => (u._1, u._2.map(r => r.rating).sum/u._2.size)).collect.toMap
  val utri = allData.groupBy(_.item).mapValues(_.map(_.user)).collect.toMap

  val itemsRatedByUsers = allData.groupBy(_.user).map(u => (u._1, u._2.map(_.item).toSet)).collect.toMap
  //val itemsRatingsByUsers
  val vbu = allData.groupBy(_.user)
    .mapValues(r => r.map(v => (v.item, normalizedDeviation(v.rating, perUserPred.getOrElse(v.user, 0.0))))) //.collect.toMap
  val norm = allData.groupBy(_.user).mapValues(v => v.map(r => normalizedDeviation(r.rating, perUserPred.getOrElse(r.user, 0.0)))).mapValues(v => math.sqrt(v.map(math.pow(_, 2)).sum))
  val itemsRatingsByUsers = vbu.join(norm).mapValues(v => v._1.map(u => (u._1, (u._2, v._2 match {case 0.0 => 0.0; case _ => u._2/v._2})))).collect.toMap

  val users = allData.groupBy(_.user).map(_._1).collect

  def computeUserSimilarities(): Array[((Int, Int), Double)] = {
    for{
      u1 <- users
      u2 <- users
      if u1 != u2
    } yield ((u1 , u2), cosineSimilarityOpti(u1 : Int, u2 : Int, itemsRatedByUsers : Map[Int, Set[Int]], itemsRatingsByUsers : Map[Int, Iterable[(Int, (Double, Double))]]))
  }

  val userSimilarities = computeUserSimilarities().groupBy(_._1._1).mapValues(_.toSeq.sortBy{ case (_, sim) => sim }(Ordering[Double].reverse))


  println("Done precomputing")
  def predictTop5MoviesUsingKNN(userId : Int, k: Int) : List[List[Any]] ={
    val sims = userSimilarities.mapValues(_.take(k).toMap).map(identity)
    val predictedItems = unratedMovies.map(r => (r._1, r._2, predictUserItemCosineOpti(userId, r._1, perUserPred, utri, itemsRatedByUsers, itemsRatingsByUsers, sims.getOrElse(userId, Map.empty[(Int, Int), Double])))).sortBy(_._3, ascending = false).map(t => List(t._1, t._2, t._3))
    //predictedItems.take(50).toList.foreach(println)
    return predictedItems.take(5).toList

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

          // IMPORTANT: To break ties and ensure reproducibility of results,
          // please report the top-5 recommendations that have the smallest
          // movie identifier.

          "Q3.2.5" -> Map(
            "Top5WithK=30" -> predictTop5MoviesUsingKNN(944, 30),

            "Top5WithK=300" -> predictTop5MoviesUsingKNN(944, 300)

            // Discuss the differences in rating depending on value of k in the report.
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
