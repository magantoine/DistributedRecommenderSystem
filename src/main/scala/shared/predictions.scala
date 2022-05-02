package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

package object predictions
{
  // ------------------------ For template
  case class Rating(user: Int, item: Int, rating: Double)


  type Dataset = List[Rating]
  type RateMatrix = CSCMatrix[Rate]
  type UserId = Int
  type ItemId = Int 
  type Rate = Double
  type Predictor = (UserId, ItemId) => Rate


  def numberOfUser(data : Dataset) : Int = {
    return data.map(_.user).toSet.size
  }

  def numberOfItems(data : Dataset) : Int = {
    return data.map(_.item).toSet.size
  }


  def shape(m : RateMatrix) : (Int, Int) = {
    return (m.rows, m.cols)
  }

  def sumAlongAxis(m : RateMatrix, axis : Int) : RateMatrix = {
    var multiplicator =  if(axis == 1) new CSCMatrix.Builder[Double](rows=m.cols, cols=1) else new CSCMatrix.Builder[Double](rows=1, cols=m.rows) 
    if(axis == 1) {
      for (i <- 0 until m.cols){
        multiplicator.add(i, 0, 1)
      }
    } else {
      for (i <- 0 until m.rows){
        multiplicator.add(0, i, 1)
      }
    }
    return if(axis == 1) m * multiplicator.result() else multiplicator.result() * m
  }


  def computeMAE(test : RateMatrix, predictor : Predictor) : Double = {
    var mae = 0.0
    var cnt = 0
    test.activeIterator.foreach({case ((uId, iId), r) => {
      mae += abs(r - predictor(uId, iId))
      cnt = cnt + 1
    }})

    return mae / cnt.toDouble
  }


   def scale(userRating : Double, userAvgRating : Double) : Double = userRating match {
    case x if x > userAvgRating => 5.0 - userAvgRating
    case x if x < userAvgRating => userAvgRating - 1.0
    case x if x == userAvgRating => 1.0
  }

  def kNNPredictor(train : RateMatrix, k : Int) : (Predictor, RateMatrix) = {

    val nbUsers = train.rows
    val nbItems = train.cols
    var maskBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)
    train.activeIterator.foreach({ case ((uId, iId), r) => {
      maskBuilder.add(uId, iId, 1)
    }})
    
    val mask = maskBuilder.result()
    val userCounts = sumAlongAxis(mask,axis=1)
    val itemCounts = sumAlongAxis(mask,axis=0)

    // compute average rating per user
    val usersAverage = sumAlongAxis(train, axis=1) /:/ userCounts


    // compute the deviations r(u, i)^ = (r(u, i)  - ravg(u)) / scale(r(u, i), ravg(u))
    var preScaleBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)
    var cnt = 0.0
    var sumOfRate = 0.0
    train.activeIterator.foreach({ case ((uId, iId), r) => {
      val uAverage = usersAverage(uId, 0)
      val scaling = scale(r, uAverage)
      preScaleBuilder.add(uId, iId, (r - uAverage) / scaling.toDouble)
      cnt += 1
      sumOfRate += r
    }})
    val devs = preScaleBuilder.result()

    

    // compute the preprocessed ratings r~(u, i) = r(u, i)^ / sqrt(sum(all the items rated by one user r^(u, j) squared ))
    val norms = sumAlongAxis(devs :* devs, axis=1)
    var preprocRatingBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)
    devs.activeIterator.foreach({case ((uId, iId), r) => {
        preprocRatingBuilder.add(uId, iId, r / math.sqrt(norms(uId, 0)))
    }})
    val preprocRatings = preprocRatingBuilder.result()



    // compute the similarities, sum for all items j rated by both r~(u, j) * r~(v, j)
    val sims = preprocRatings * preprocRatings.t


    // keep only the topKSims for each users
    var topKSimsBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbUsers)
    val tops = argtopk(sims.toDense(::, *), k + 1)
    for (uId <- 0 until train.rows){
      for (vId <- tops(uId)){
          if(uId != vId) {
            topKSimsBuilder.add(uId, vId, sims(uId, vId))
          }
      }
    }

    val topKSims = topKSimsBuilder.result()

    
    // denoms : denoms(u, i) = sum for all the items v that rated i of |s(u, v)|
    val denoms = abs(topKSims) * mask
    val nums = topKSims * devs 


    
    var userItemDevBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)
    nums.activeIterator.foreach({case ((uId, iId), num) => {
        userItemDevBuilder.add(uId, iId, num / denoms(uId, iId).toDouble)
    }})

    val userItemDevs = userItemDevBuilder.result()

    val userItemPairs = for(uId <- 0 until train.rows; iId <- 0 until train.cols) yield (uId, iId)

    
    var predictionsBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)
    
    
    userItemPairs.foreach({ case (uId, iId) => {
      val uAverage = usersAverage(uId, 0)
      val dev = userItemDevs(uId, iId)
      val scaling = scale(dev + uAverage, uAverage)
      predictionsBuilder.add(uId, iId, uAverage + dev * scaling)
      }
    })
    
    val predictions = predictionsBuilder.result()
    val defaultValue = sumOfRate / cnt

    
    val itemAvgDev = (sumAlongAxis(devs, axis=0).t /:/ itemCounts.t).t

    return ((uId : UserId, iId : ItemId) => {
      val out = predictions(uId, iId)
      if(userCounts(uId, 0) == 0){
        defaultValue
      } else {
        out
      }
    }, topKSims)

  }

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0

  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else { 
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble) 
    }
  }


  def load(path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = Source.fromFile(path)
    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies) 
    for (line <- file.getLines) {
      val cols = line.split(sep).map(_.trim)
      toInt(cols(0)) match {
        case Some(_) => builder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
        case None => None
      }
    }
    file.close
    builder.result()
  }

  def loadSpark(sc : org.apache.spark.SparkContext,  path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = sc.textFile(path)
    val ratings = file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) => Some(((cols(0).toInt-1, cols(1).toInt-1), cols(2).toDouble))
          case None => None
        }
      })
      .filter({ case Some(_) => true
                 case None => false })
      .map({ case Some(x) => x
             case None => ((-1, -1), -1) }).collect()

    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies)
    for ((k,v) <- ratings) {
      v match {
        case d: Double => {
          val u = k._1
          val i = k._2
          builder.add(u, i, d)
        }
      }
    }
    return builder.result
  }

  def partitionUsers (nbUsers : Int, nbPartitions : Int, replication : Int) : Seq[Set[Int]] = {
    val r = new scala.util.Random(1337)
    val bins : Map[Int, collection.mutable.ListBuffer[Int]] = (0 to (nbPartitions-1))
       .map(p => (p -> collection.mutable.ListBuffer[Int]())).toMap
    (0 to (nbUsers-1)).foreach(u => {
      val assignedBins = r.shuffle(0 to (nbPartitions-1)).take(replication)
      for (b <- assignedBins) {
        bins(b) += u
      }
    })
    bins.values.toSeq.map(_.toSet)
  }
   def getTimings(function : () => Double, num_measurements : Int) : Array[Double] = {
    val res =  for (i <- 0 to num_measurements) yield { timingInMs(function) }

    return res.toArray.map(_._2)
  }

}


