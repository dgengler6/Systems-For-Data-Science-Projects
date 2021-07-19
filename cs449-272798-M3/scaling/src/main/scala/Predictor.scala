import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String]()
  verify()
}

object Predictor {
  def main(args: Array[String]) {
    var conf = new Conf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("Loading training data from: " + conf.train())
    val read_start = System.nanoTime
    val trainFile = Source.fromFile(conf.train())
    val trainBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- trainFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        trainBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val train = trainBuilder.result()
    trainFile.close
    val read_duration = System.nanoTime - read_start
    println("Read data in " + (read_duration/pow(10.0,9)) + "s")

    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()
    println("Compute kNN on train data...")

    // Utilities

    def scale(x : Double, r : Double): Double = (x,r) match{
      case (xs,rs) if xs > rs => 5 - rs
      case (xs,rs) if xs < rs => rs - 1
      case _ => 1
    }

    def normalizedDeviation(currentValue : Double, userAverage: Double): Double = (currentValue - userAverage) / scale(currentValue, userAverage)

    // Preprocess

    // Start time measure for KNN :

    val startTimeKNN = System.currentTimeMillis()

    // Compute the userAverage for normalization and prediction
    val userAverage = DenseVector.zeros[Double](conf_users)
    val userAverageSize = DenseVector.zeros[Double](conf_users)

    for (((user, _),v) <- train.activeIterator) {
      userAverage(user) += v
      userAverageSize(user) += 1
    }

    for ((user, v) <- userAverage.activeIterator) {
      userAverage(user) = v / userAverageSize(user)
    }


    // Compute Normalized deviation for ratings
    val builderRatings = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_movies)

    for (((user, item), rating) <- train.activeIterator){
      builderRatings.add(user, item, normalizedDeviation(rating, userAverage(user)))
    }

    val ratings = builderRatings.result()


    // Compute the normalization factor in order to compute sim
    val normalization_1 = DenseVector.zeros[Double](conf_users)

    for (((user, _),v) <- ratings.activeIterator) {
      normalization_1(user) += v * v
    }
    val normalization = sqrt(normalization_1)
    //for ((user, v) <- normalization.activeIterator) {
    //  normalization(user) = math.sqrt(v)
    //}

    // Compute the similarities
    val builderRatingsPreprocessed = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_movies)

    for (((user, movie),v) <- ratings.activeIterator) {

      builderRatingsPreprocessed.add(user, movie, normalization(user) match{case(n) if n==0.0 => 0.0; case(_) => v / normalization(user)})
    }

    val ratingsPreprocessed = builderRatingsPreprocessed.result()

    // Broadcast

    val br = sc.broadcast(ratingsPreprocessed)

    // Procedure topk

    def topk(user : Int): (Int, IndexedSeq[(Int, Double)]) = {
      val rPrep =  br.value
      val ratingsUser = rPrep(user, 0 until conf_movies)

      val simForUser = rPrep * ratingsUser.t

      simForUser(user) = 0.0

      val topK = argtopk(simForUser, conf_k)

      return (user, topK.map{case u2 => (u2, simForUser(u2))})
    }

    // Parallelize

    val topks = sc.parallelize(0 until conf_users).map(topk).collect()

    // Build kNN
    val builderKnn = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_users)

    for ((u1, vector) <- topks) {
      for ((u2, sim) <- vector){
        builderKnn.add(u1, u2, sim)
      }
    }

    val knnSim = builderKnn.result()

    val endTimeKNN = System.currentTimeMillis()

    val knnDuration = endTimeKNN - startTimeKNN

    println("Loading test data from: " + conf.test())
    val testFile = Source.fromFile(conf.test())
    val testBuilder = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies()) 
    for (line <- testFile.getLines) {
        val cols = line.split(conf.separator()).map(_.trim)
        testBuilder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
    }
    val test = testBuilder.result()
    testFile.close

    println("Compute predictions on test data...")

    val startTimePred = System.currentTimeMillis()

    def compute_usws_deviation(user : Int, item : Int, similarities : CSCMatrix[Double], ratings : CSCMatrix[Double]) : Double = {

      val userSimilarities = similarities(user, 0 until conf_users)


      val movieRatings = ratings(0 until conf_users, item)

      val numeratorSum =  userSimilarities *  movieRatings

      val sim_rated_i = DenseVector.zeros[Double](conf_users)

      for ((u, v) <- movieRatings.activeIterator){
        if (v != 0.0) {
          sim_rated_i(u) = userSimilarities(u)
        }
      }
      val denominatorSum = sum(abs(sim_rated_i))

      if (denominatorSum == 0.0) {
        0.0
      } else {
        numeratorSum / denominatorSum
      }
    }
    //spark-submit --master yarn --num-executors 1 target/scala-2.11/m3_yourid-assembly-1.0.jar --train ../data/ml-1m/ra.train     --test ../data/ml-1m/ra.test --k 200 --json scaling-1m.json --users 6040 --movies 3952 --separator "::"
    val br_knn = sc.broadcast(knnSim)
    val br_avg = sc.broadcast(userAverage)
    val br_ratings = sc.broadcast(ratings)

    def predict_rating(pair_user_item : (Int, Int)): (Int, Int, Double) = {
      val (user, item) = pair_user_item

      val ratings =  br_ratings.value
      val knnSim = br_knn.value
      val avgAllUsers = br_avg.value

      val avgUser = avgAllUsers(user)
      val uswsDeviation = compute_usws_deviation(user, item, knnSim, ratings)

      val prediction = avgUser + uswsDeviation * scale(avgUser + uswsDeviation, avgUser)

      return (user, item, prediction)
    }

    val predictions = sc.parallelize(test.activeIterator.map(_._1).toSeq).map(predict_rating).collect()

    val builderPred = new CSCMatrix.Builder[Double](rows=conf_users, cols=conf_movies)

    for ((user, item, pred) <- predictions) {
      builderPred.add(user, item, pred)
    }

    val predMatrix = builderPred.result()

    val endTimePred = System.currentTimeMillis()

    val predDuration = endTimePred - startTimePred

    val mae = sum(abs(predMatrix - test)) / test.activeSize

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
            "Q4.1.1" -> Map(
              "MaeForK=200" -> mae  // Datatype of answer: Double
            ),
            // Both Q4.1.2 and Q4.1.3 should provide measurement only for a single run
            "Q4.1.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> knnDuration  // Datatype of answer: Double
            ),
            "Q4.1.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> predDuration // Datatype of answer: Double
            )
            // Answer the other questions of 4.1.2 and 4.1.3 in your report
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  } 
}
