import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

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
    println("")
    println("******************************************************")

    var conf = new Conf(args)

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

    println("Compute kNN on train data...")

    // Utilities

    def scale(x : Double, r : Double): Double = (x,r) match{
      case (xs,rs) if xs > rs => 5 - rs
      case (xs,rs) if xs < rs => rs - 1
      case _ => 1
    }

    def normalizedDeviation(currentValue : Double, userAverage: Double): Double = (currentValue - userAverage) / scale(currentValue, userAverage)


    // Compute the userAverage for normalization and prediction
    val userAverage = DenseVector.zeros[Double](conf.users())
    val userAverageSize = DenseVector.zeros[Double](conf.users())

    for (((user, _),v) <- train.activeIterator) {
      userAverage(user) += v
      userAverageSize(user) += 1
    }

    for ((user, v) <- userAverage.activeIterator) {
      userAverage(user) = v / userAverageSize(user)
    }


    // Compute Normalized deviation for ratings
    val builderRatings = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())

    for (((user, item), rating) <- train.activeIterator){
      builderRatings.add(user, item, normalizedDeviation(rating, userAverage(user)))
    }

    val ratings = builderRatings.result()


    // Compute the normalization factor in order to compute sim
    val normalization_1 = DenseVector.zeros[Double](conf.users())

    for (((user, _),v) <- ratings.activeIterator) {
      normalization_1(user) += v * v
    }
    val normalization = sqrt(normalization_1)
    //for ((user, v) <- normalization.activeIterator) {
    //  normalization(user) = math.sqrt(v)
    //}

    // Compute the similarities
    val builderTmpSim = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())

    for (((user, movie),v) <- ratings.activeIterator) {

      builderTmpSim.add(user, movie, normalization(user) match{case(n) if n==0.0 => 0.0; case(_) => v / normalization(user)})
    }

    val simTmp = builderTmpSim.result()

    // If we use the 100k dataset we want to compute the whole similarity matrix
    // otherwise we pass to get_knn the temporary sim matrix and compute argtopk on a matrix vector multiplication
    val similarities = if (conf.users() < 1000 ) {
       val sim = simTmp * simTmp.t

      // Set self-similarities to zero.
      for (((u1, u2), _) <- sim.activeIterator) {
        if (u1 == u2) {
          sim(u1, u2) = 0.0
        }
      }
      sim
    }else {
      simTmp
    }

    def get_knn_similarities(k: Int = 100, similarities : CSCMatrix[Double]): CSCMatrix[Double] ={
      // Build a new similarities sparse matrix where we store only the top k similarities
      val builderKnn = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.users())

      for (user <- 0 until conf.users()){
        // Compute Top-k for user u
        val topKsimU = argtopk(similarities(user, 0 until conf.users()).t, k )

        // Fill the similarity matrix with only the similarities that interest us
        for (i <- topKsimU){
          builderKnn.add(user, i, similarities(user, i))
        }
      }
      builderKnn.result()
    }

    // We get use this to compute knn if the dataset is too big
    def get_knn_similarities_1M(k: Int = 100, similarities : CSCMatrix[Double]): CSCMatrix[Double] ={
      // Build a new similarities sparse matrix where we store only the top k similarities
      val builderKnn = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.users())

      for (user <- 0 until conf.users()){
        // Compute Top-k for user u
        val simU = similarities * similarities(user, 0 until conf.movies()).t
        simU(user) = 0.0
        val topKsimU = argtopk(simU, k)

        // Fill the similarity matrix with only the similarities that interest us
        for (i <- topKsimU){
          builderKnn.add(user, i, simU(i))
        }
      }
      builderKnn.result()
    }

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

    // Computes the user-specific weighted-sum deviation for item i
    def compute_usws_deviation(user : Int, item : Int, similarities : CSCMatrix[Double]) : Double = {

      val userSimilarities = similarities(user, 0 until conf.users())


      val movieRatings = ratings(0 until conf.users(), item)

      val numeratorSum =  userSimilarities *  movieRatings

      val sim_rated_i = DenseVector.zeros[Double](conf.users())

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


    // Predict rating of item by user
    def predictRating(user : Int, item : Int, similarities : CSCMatrix[Double]) : Double = {
      val avgUser = userAverage(user)

      val uswsDeviation = compute_usws_deviation(user, item, similarities)


      avgUser + uswsDeviation * scale(avgUser + uswsDeviation, avgUser)
    }


    def predictTestK(k : Int): Double ={
      val sim_k = if (conf.users() < 1000) {
        get_knn_similarities(k, similarities)
      }else{
        get_knn_similarities_1M(k, similarities)
      }

      val mae = test.activeIterator.map{case((user, item),target) => math.abs(target - predictRating(user , item , sim_k))}.sum / test.activeIterator.length.toDouble

      println(mae)
      mae
    }

    def knnBenchmark(): CSCMatrix[Double] = {
      val n1 = DenseVector.zeros[Double](conf.users())
      for (((user, _),v) <- ratings.activeIterator) {n1(user) += v * v}
      val n = sqrt(n1)
      val builderSim = new CSCMatrix.Builder[Double](rows=conf.users(), cols=conf.movies())
      for (((user, movie),v) <- ratings.activeIterator) {builderSim.add(user, movie, n(user) match{case(p) if p==0.0 => 0.0; case(_) => v / n(user)})}
      val simt = builderSim.result()
      if(conf.users() < 1000){
        val sim = simt * simt.t
        get_knn_similarities(conf.k(),sim)
      }else{
        val sim = simt
        get_knn_similarities_1M(conf.k(),sim)
      }

    }
    // sbt "run --train ../data/ml-100k/u1.base --test ../data/ml-100k/u1.test --k 100 --json optimizing.json --users 943 --movies 1682 --separator \"\t\""
    // sbt "run --train ../data/ml-1m/ra.train --test ../data/ml-1m/ra.test --k 200 --json scaling-1m.json --users 6040 --movies 3952 --separator "::""
    // spark-submit --master MASTER target/scala-2.11/m3_yourid-assembly-1.0.jar --train ../data/ml-1m/ra.train     --test ../data/ml-1m/ra.test --k 200 --json scaling-1m.json --users 6040 --movies 3952 --separator "::"

    def computeKNNDuration(): (Double, Double, Double, Double) = {

      val iterations = for {
        _ <- 1 to 5
        startTime = System.currentTimeMillis()
        _ = knnBenchmark()
        endTime = System.currentTimeMillis()
        duration = endTime - startTime
      } yield duration

      val minKNN = iterations.min
      val maxKNN = iterations.max
      val avgKNN = iterations.sum / iterations.size
      val stdKNN = scala.math.sqrt( iterations.map(x => scala.math.pow(x - avgKNN, 2)).sum / iterations.size )
      (minKNN, maxKNN, avgKNN, stdKNN)
    }

    val (minKNN, maxKNN, avgKNN, stdKNN) = computeKNNDuration()


    def computePredictionDuration(): (Double, Double, Double, Double) = {

      val sim_k = if (conf.users() < 1000) {
        get_knn_similarities(conf.k(), similarities)
      }else{
        get_knn_similarities_1M(conf.k(), similarities)
      }

      val iterations = for {
        _ <- 1 to 5
        startTime = System.currentTimeMillis()
        _ = for (((user, item), _ )<- test.activeIterator) {predictRating (user, item, sim_k)}
        endTime = System.currentTimeMillis()
        duration = endTime - startTime
      } yield duration

      val minPred = iterations.min
      val maxPred = iterations.max
      val avgPred = iterations.sum / iterations.size
      val stdPred = scala.math.sqrt( iterations.map(x => scala.math.pow(x - avgPred, 2)).sum / iterations.size )
      (minPred, maxPred, avgPred, stdPred)
    }

    val (minPred, maxPred, avgPred, stdPred) = computePredictionDuration()

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
            "Q3.3.1" -> Map(
              "MaeForK=100" -> predictTestK(100), // Datatype of answer: Double
              "MaeForK=200" -> predictTestK(200)  // Datatype of answer: Double
            ),
            "Q3.3.2" ->  Map(
              "DurationInMicrosecForComputingKNN" -> Map(
                "min" -> minKNN,  // Datatype of answer: Double
                "max" -> maxKNN, // Datatype of answer: Double
                "average" -> avgKNN, // Datatype of answer: Double
                "stddev" -> stdKNN // Datatype of answer: Double
              )
            ),
            "Q3.3.3" ->  Map(
              "DurationInMicrosecForComputingPredictions" -> Map(
                "min" -> minPred,  // Datatype of answer: Double
                "max" -> maxPred, // Datatype of answer: Double
                "average" -> avgPred, // Datatype of answer: Double
                "stddev" -> stdPred // Datatype of answer: Double
              )
            )
            // Answer the Question 3.3.4 exclusively on the report.
           )
          json = Serialization.writePretty(answers)
        }

        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}
