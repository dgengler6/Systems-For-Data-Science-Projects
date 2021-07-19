 package stats

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Analyzer extends App {
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

  // CODE
  val globalAverageRating = data.map(r => r.rating).mean()

  def usersAverageRating(globalAverage : Double): (Double, Double, Double, Double)= {
    val usersRating = data.groupBy(r => r.user).map(u => u._2.map(r => r.rating).sum/u._2.size)

    val min = usersRating.min()

    val max = usersRating.max()

    val average = usersRating.mean()

    val ratio = usersRating.filter(r => scala.math.abs(r - globalAverage) < 0.5).count / usersRating.count.toDouble

    return (min, max, average, ratio)
  }

  val (minUsersAverageRating, maxUsersAverageRating, avgUsersAverageRating, ratioUsersClose) = usersAverageRating(globalAverageRating)
  //println(s"Min $minUsersAverageRating, Max $maxUsersAverageRating, Avg $avgUsersAverageRating, Ratio $ratioUsersClose")


  def itemsAverageRating(globalAverage : Double): (Double, Double, Double, Double)= {
    val itemsRating = data.groupBy(r => r.item).map(u => u._2.map(r => r.rating).sum/u._2.size)

    val min = itemsRating.min()

    val max = itemsRating.max()

    val average = itemsRating.mean()

    val ratio = itemsRating.filter(r => scala.math.abs(r - globalAverage) < 0.5).count / itemsRating.count.toDouble

    return (min, max, average, ratio)
  }

  val (minItemsAverageRating, maxItemsAverageRating, avgItemsAverageRating, ratioItemsClose) = itemsAverageRating(globalAverageRating)
  //println(s"Min $minItemsAverageRating, Max $maxItemsAverageRating, Avg $avgItemsAverageRating, Ratio $ratioItemsClose")

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
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> globalAverageRating // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                // Using as your input data the average rating for each user,
                // report the min, max and average of the input data.
                "min" -> minUsersAverageRating,  // Datatype of answer: Double
                "max" -> maxUsersAverageRating, // Datatype of answer: Double
                "average" -> avgUsersAverageRating // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> false, // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> ratioUsersClose // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> minItemsAverageRating,  // Datatype of answer: Double
                "max" -> maxItemsAverageRating, // Datatype of answer: Double
                "average" -> avgItemsAverageRating // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> false, // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> ratioItemsClose // Datatype of answer: Double
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
