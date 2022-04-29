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


  def kNNPredictor(train : RateMatrix) : Predictor = {
    // first we need to build the breeze matrix

    //val rates = buildSparseMatrix(train)

    // compute average rating per user
    println("shape of train")
    println(shape(train))

    // this doesn't work with a sparse matrix
    //val usersAverage = mean(train(::, *))
    val usersAverage = sumAlongAxis(train, axis=1) /:/ train.rows.toDouble


    println("shape of usersAverage")
    println(shape(usersAverage))
    
    

    // computing devs :
    var preScaleBuilder = new CSCMatrix.Builder[Rate](rows=train.rows, cols=train.cols)

    train.activeIterator.foreach({ case ((uId, iId), r) => {
      val uAverage = usersAverage(uId, 0)
      val scaling = (r - uAverage) match {
        case x if x < 0 => uAverage - 1
        case x if x > 0 => 5 - uAverage
      }
      preScaleBuilder.add(uId, iId, (r - uAverage) / scaling)
    }})

    val devs = preScaleBuilder.result()


    val averageItemDevs = sumAlongAxis(devs, axis=0) /:/ devs.cols.toDouble

    println("shape of devs")
    println(shape(devs))

    println("shape of average item devs")
    print(shape(averageItemDevs))


    val norms = sumAlongAxis(devs :* devs, axis=0)

    var preprocRatingBuilder = new CSCMatrix.Builder[Rate](rows=train.rows, cols=train.cols)

    devs.activeIterator.foreach({case ((uId, iId), r) => {
      preprocRatingBuilder.add(uId, iId, r / math.sqrt(norms(0, iId)))
    }})

    val preprocRatings = preprocRatingBuilder.result()

    println("Preproc rating shape : ")
    println(shape(preprocRatings))


    val sims = preprocRatings.t * preprocRatings

    println("preproc ratings shape : ")
    println(shape(preprocRatings))

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


