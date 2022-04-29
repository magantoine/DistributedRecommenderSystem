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


  def kNNPredictor(train : RateMatrix) : Predictor = {
    // first we need to build the breeze matrix

    //val rates = buildSparseMatrix(train)

    // compute average rating per user
    println(shape(train))
    //val usersAverage = mean(train(::, *))
    
    // computing devs :
    // var preScale = train - usersAverage
    // preScale(I(preScale == 0)) = 1
    // preScale(I(preScale > 0)) = -(usersAverage - 5)
    // preScale(I(preScale < 0)) = usersAverage - 1
    
    // val ratingDevs = (train - usersAverage) / preScale

    // val itemsDevs = mean(ratingDevs(::, *))
    // itemsDevs.take(10).foreach(println)

    // // computing similarities
    // val r_tilds = ratingDevs / norm(ratingDevs(*, ::))
    // r_tilds.take(10).foreach(println)
    // // simple matrix multiplication in numpy @
    // val sims = r_tilds * r_tilds 

    // sims.take(10).foreach(println)

    return (x, y) => 1

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


}


