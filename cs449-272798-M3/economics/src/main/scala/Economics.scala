import org.rogach.scallop._
import org.json4s.jackson.Serialization
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)


    // 5.1.1
    val priceICCM7 = 35000.0

    val dailyCostICCM7 = 20.40

    val nbDayRenting = round(priceICCM7 / dailyCostICCM7).toDouble

    val nbYearsRenting = nbDayRenting / 365

    // 5.1.2

    val RAMM7 = 24 * 64

    val CPUM7 = 2 * 14

    val price1GBRAMperDay = 0.012

    val priceCPUperDay = 0.088

    val dailyCostContainerEqM7 = RAMM7 * price1GBRAMperDay + CPUM7 * priceCPUperDay

    val ratioM7overContainer = dailyCostICCM7 / dailyCostContainerEqM7

    val containerCheaperM7 = (ratioM7overContainer > 1.0) && (math.abs(ratioM7overContainer - 1.0) > 0.05)

    // 5.1.3

    val RAM1RPi = 8

    val RAM4RPi = 4 * RAM1RPi

    val CPU4PRi = 1

    // Daily cost for RPi at min and max power
    val priceRPisMaxPowerPerDay = 0.054

    val priceRPisMinPowerPerDay = 0.0108

    val dailyCost4RPiMax = 4 * priceRPisMaxPowerPerDay

    val dailyCost4RPiMin = 4 * priceRPisMinPowerPerDay

    // daily cost for container equivalent to 4RPi
    val dailyCostContainerEq4RPi = RAM4RPi * price1GBRAMperDay + CPU4PRi * priceCPUperDay

    val ratio4RPioverContainerMax = dailyCost4RPiMax / dailyCostContainerEq4RPi

    val ratio4RPioverContainerMin = dailyCost4RPiMin / dailyCostContainerEq4RPi

    val containerCheaperRPi = (ratio4RPioverContainerMax > 1.0) && (math.abs(ratio4RPioverContainerMax - 1.0) > 0.05)


    // 5.1.4

    val priceRPi = 94.83

    val minNumberDaysMinPower = round((4 * priceRPi) / (dailyCostContainerEq4RPi - dailyCost4RPiMin)).toDouble

    val minNumberDaysMaxPower = round((4 * priceRPi) / (dailyCostContainerEq4RPi - dailyCost4RPiMax)).toDouble

    // 5.1.5

    val nbRPiForM7Cost = floor(priceICCM7 / priceRPi)

    val ratioRAMRPioverM7 = (nbRPiForM7Cost * RAM1RPi) / RAMM7

    val ratioThroughputRPioverM7 = (nbRPiForM7Cost * 0.25) / CPUM7

    // 5.1.6

    val avgMoviePerUser = 100

    val bytesPerSim = 4

    val bytesPerRatings = 1

    val optimalK = 200

    val bytesPerUser = avgMoviePerUser * bytesPerRatings + optimalK * bytesPerSim

    val bytesPerGB = 1000000000

    val nbUserPerGB = bytesPerGB / bytesPerUser / 2

    val nbUserPerRpi = RAM1RPi * nbUserPerGB

    val nbUserPerM7 = RAMM7 * nbUserPerGB

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
            "Q5.1.1" -> Map(
              "MinDaysOfRentingICC.M7" -> nbDayRenting, // Datatype of answer: Double
              "MinYearsOfRentingICC.M7" -> nbYearsRenting // Datatype of answer: Double
            ),
            "Q5.1.2" -> Map(
              "DailyCostICContainer_Eq_ICC.M7_RAM_Throughput" -> dailyCostContainerEqM7, // Datatype of answer: Double
              "RatioICC.M7_over_Container" -> ratioM7overContainer, // Datatype of answer: Double
              "ContainerCheaperThanICC.M7" -> containerCheaperM7 // Datatype of answer: Boolean
            ),
            "Q5.1.3" -> Map(
              "DailyCostICContainer_Eq_4RPi4_Throughput" -> dailyCostContainerEq4RPi, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MaxPower" -> ratio4RPioverContainerMax, // Datatype of answer: Double
              "Ratio4RPi_over_Container_MinPower" -> ratio4RPioverContainerMin, // Datatype of answer: Double
              "ContainerCheaperThan4RPi" -> containerCheaperRPi // Datatype of answer: Boolean
            ),
            "Q5.1.4" -> Map(
              "MinDaysRentingContainerToPay4RPis_MinPower" -> minNumberDaysMinPower, // Datatype of answer: Double
              "MinDaysRentingContainerToPay4RPis_MaxPower" -> minNumberDaysMaxPower // Datatype of answer: Double
            ),
            "Q5.1.5" -> Map(
              "NbRPisForSamePriceAsICC.M7" -> nbRPiForM7Cost, // Datatype of answer: Double
              "RatioTotalThroughputRPis_over_ThroughputICC.M7" -> ratioThroughputRPioverM7, // Datatype of answer: Double
              "RatioTotalRAMRPis_over_RAMICC.M7" -> ratioRAMRPioverM7 // Datatype of answer: Double
            ),
            "Q5.1.6" ->  Map(
              "NbUserPerGB" -> nbUserPerGB, // Datatype of answer: Double
              "NbUserPerRPi" -> nbUserPerRpi, // Datatype of answer: Double
              "NbUserPerICC.M7" -> nbUserPerM7 // Datatype of answer: Double
            )
            // Answer the Question 5.1.7 exclusively on the report.
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
