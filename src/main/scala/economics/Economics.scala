import org.rogach.scallop._
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._

package economics {

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val json = opt[String]()
  verify()
}

object Economics {
  def main(args: Array[String]) {
    println("")
    println("******************************************************")

    var conf = new Conf(args)

    // Save answers as JSON
    def printToFile(content: String,
                    location: String = "./answers.json") =
      Some(new java.io.PrintWriter(location)).foreach{
        f => try{
          f.write(content)
        } finally{ f.close }
    }



    // E.1//
    val IccRenting = 20.4
    val IccPrice = 38600
    val IccRam = 24*64
    val IccNumCores = 2*14


    val MinRentingDays = (IccPrice/IccRenting).ceil

    // E.2//

    val numSecondsPerDay = 24*60*60
    val numRPi = 4
    val ramRPi = 8
    val pricePi = 108.48
    val totalPrice = numRPi*pricePi
    val powerIdle = 3
    val powerComputing = 4
    val energyCostPerKwh = 0.25

    def numKwhUsedInDay(wattUsage:Double): Double = {
      return (wattUsage/1000)*24
    }


    val numGB = numRPi*ramRPi
    val numCPU = (numRPi/4).ceil
    val costGB = 1.6E-7
    val costCPU = 1.14E-6

    val ContainerDailyCost = numSecondsPerDay*(numGB*costGB + numCPU*costCPU)
    val RPisDailyCostIdle = numRPi*numKwhUsedInDay(powerIdle)*energyCostPerKwh
    val RPisDailyCostComputing = numRPi*numKwhUsedInDay(powerComputing)*energyCostPerKwh

    val MinRentingDaysIdleRPiPower = (totalPrice/(ContainerDailyCost - RPisDailyCostIdle)).ceil
    val MinRentingDaysComputingRPiPower = (totalPrice/(ContainerDailyCost - RPisDailyCostComputing)).ceil

    val NbRPisEqBuyingICCM7 = (IccPrice/pricePi).floor
    val RatioRAMRPisVsICCM7 = NbRPisEqBuyingICCM7*ramRPi / IccRam
    val numRPiForOneIntelCore = 4
    val RatioComputeRPisVsICCM7 = NbRPisEqBuyingICCM7/(IccNumCores*numRPiForOneIntelCore)


    conf.json.toOption match {
      case None => ;
      case Some(jsonFile) => {

        val answers = ujson.Obj(
          "E.1" -> ujson.Obj(
            "MinRentingDays" -> ujson.Num(MinRentingDays) // Datatype of answer: Double
          ),
          "E.2" -> ujson.Obj(
            "ContainerDailyCost" -> ujson.Num(ContainerDailyCost),
            "4RPisDailyCostIdle" -> ujson.Num(RPisDailyCostIdle),
            "4RPisDailyCostComputing" -> ujson.Num(RPisDailyCostComputing),
            "MinRentingDaysIdleRPiPower" -> ujson.Num(MinRentingDaysIdleRPiPower),
            "MinRentingDaysComputingRPiPower" -> ujson.Num(MinRentingDaysComputingRPiPower) 
          ),
          "E.3" -> ujson.Obj(
            "NbRPisEqBuyingICCM7" -> ujson.Num(NbRPisEqBuyingICCM7),
            "RatioRAMRPisVsICCM7" -> ujson.Num(RatioRAMRPisVsICCM7),
            "RatioComputeRPisVsICCM7" -> ujson.Num(RatioComputeRPisVsICCM7  )
          )
        )

        val json = write(answers, 4)
        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
  } 
}

}
