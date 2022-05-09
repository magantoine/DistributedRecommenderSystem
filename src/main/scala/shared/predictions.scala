package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.In

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

// PART 1 BREEZE

  def numberOfUser(data : Dataset) : Int = {
    return data.map(_.user).toSet.size
  }

  def numberOfItems(data : Dataset) : Int = {
    return data.map(_.item).toSet.size
  }


  def shape(m : RateMatrix) : (Int, Int) = {
    return (m.rows, m.cols)
  }

  def sumAlongAxis(m : RateMatrix, axis : Int) : CSCMatrix[Double] = {
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




  def globalAverageAndUsersAverage(train: CSCMatrix[Double]): (CSCMatrix[Double], Double) = {
    val nbUsers = train.rows
    val nbItems = train.cols
    var maskBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)

    var cnt = 0.0
    var sumOfRate = 0.0

    train.activeIterator.foreach({ case ((uId, iId), r) => {
      maskBuilder.add(uId, iId, 1)
      cnt += 1
      sumOfRate += r
    }})

    val globalAverage = sumOfRate / cnt

    
    val mask = maskBuilder.result()

    val userCounts = sumAlongAxis(mask,axis=1)

    // compute average rating per user
    val usersAverage = sumAlongAxis(train, axis=1) /:/ userCounts

    return (usersAverage, globalAverage)
  }

  def preprocessRatings(train: CSCMatrix[Double], usersAverage: CSCMatrix[Double]): (CSCMatrix[Double], CSCMatrix[Double]) = {

    val nbUsers = train.rows
    val nbItems = train.cols

        // compute the deviations r(u, i)^ = (r(u, i)  - ravg(u)) / scale(r(u, i), ravg(u))
    var preScaleBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)

    train.activeIterator.foreach({ case ((uId, iId), r) => {
      val uAverage = usersAverage(uId, 0)
      val scaling = scale(r, uAverage)
      preScaleBuilder.add(uId, iId, (r - uAverage) / scaling.toDouble)

    }})
    val devs = preScaleBuilder.result()

    // compute the preprocessed ratings r~(u, i) = r(u, i)^ / sqrt(sum(all the items rated by one user r^(u, j) squared ))
    //val norms = sqrt(sumAlongAxis(devs :* devs, axis=1))
    val norms = sqrt((devs *:* devs) * DenseVector.ones[Double](devs.cols))
    
    var preprocRatingBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)
    devs.activeIterator.foreach({case ((uId, iId), r) => {
        preprocRatingBuilder.add(uId, iId, r / norms(uId))
    }})
    
    val preprocRatings = preprocRatingBuilder.result()

    return (preprocRatings, devs)
  }

  def computeKTopSimilarites(preprocRatings:CSCMatrix[Double], k:Int): CSCMatrix[Double] = {

    
    val nbUsers = preprocRatings.rows
    val nbItems = preprocRatings.cols
    // compute the similarities, sum for all items j rated by both r~(u, j) * r~(v, j)
    val sims = preprocRatings * preprocRatings.t


    // keep only the topKSims for each users
    var topKSimsBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbUsers)
    val tops = argtopk(sims.toDense(::, *), k + 1)
    for (uId <- 0 until nbUsers){
      for (vId <- tops(uId)){
          if(uId != vId) {
            topKSimsBuilder.add(uId, vId, sims(uId, vId))
          }
      }
    }

    val topKSims = topKSimsBuilder.result()

    return topKSims
  }


  def userItemDeviations(train: CSCMatrix[Double], topKSims:CSCMatrix[Double], devs: CSCMatrix[Double] ): CSCMatrix[Double] = {

    val nbUsers = train.rows
    val nbItems = train.cols

    var maskBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)

    train.activeIterator.foreach({ case ((uId, iId), r) => {
      maskBuilder.add(uId, iId, 1)
    }})
    
    val mask = maskBuilder.result()

    val denoms = (abs(topKSims) * mask) 
    val nums = topKSims * devs 

    
    var userItemDevBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)
    nums.activeIterator.foreach({case ((uId, iId), num) => {
        userItemDevBuilder.add(uId, iId, num / denoms(uId, iId).toDouble)
    }})

    val userItemDevs = userItemDevBuilder.result()

    return userItemDevs
  }

  def computeUserItemAverageDev(user: Int,item:Int, train: CSCMatrix[Double], topKSims:CSCMatrix[Double], devs:CSCMatrix[Double]): Double = {

    val nbUsers = train.rows
    val nbItems = train.cols

    

    val itemRatings = train(0 until nbUsers, item).toDenseVector.map(x=> if(x!= 0.0) 1.0  else 0.0) 

    val denom = itemRatings.t * abs(topKSims(user, 0 until nbUsers).t.toDenseVector)
    val num =   topKSims(user, 0 until nbUsers) * devs(0 until nbUsers, item)

    val res = if(denom != 0.0 ) num / denom else 0.0

    return res
  }


  

  def predictKNN(user:Int, item:Int, train:CSCMatrix[Double], topKSims: CSCMatrix[Double], 
  usersAverage: CSCMatrix[Double], devs: CSCMatrix[Double], globalAverage:Double): Double = {

    val nbUsers = train.rows
    val userItemAverageDev = computeUserItemAverageDev(user,item, train, topKSims, devs)
    val itemRatings = train(0 until nbUsers, item).toDenseVector.map(x=> if(x!= 0.0) 1.0  else 0.0)
    val num_item_ratings = sum(itemRatings)
    val userAverage = usersAverage(user,0)

    if(userItemAverageDev == 0.0 || num_item_ratings == 0.0) return userAverage
    else{
      val num_user_sims = sum(topKSims(user, 0 until nbUsers).t.toDenseVector.map(x=> if(x!= 0.0) 1.0  else 0.0))
      if (num_user_sims == 0.0) {return globalAverage}

      else {
      // println(s"User $user Item $item userAverage $userAverage userItemAverageDev  $userItemAverageDev num_item_ratings  $num_item_ratings")
      return userAverage + userItemAverageDev* scale(userAverage + userItemAverageDev, userAverage)
    }
    }
  }

  def kNNPredictor(train : RateMatrix, k : Int) : (Predictor, RateMatrix) = {

    val (usersAverage, globalAverage) = globalAverageAndUsersAverage(train)
    val (preprocRatings,devs) = preprocessRatings(train,usersAverage)
    val topKSims = computeKTopSimilarites(preprocRatings,k)

    //val userItemDevs = userItemDeviations(train = train, devs = devs, topKSims =  topKSims)
    //val userItemPairs = for(uId <- 0 until train.rows; iId <- 0 until train.cols) yield (uId, iId)
    //var predictionsBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)  
    //userItemPairs.foreach({ case (uId, iId) => {
    //  val uAverage = usersAverage(uId, 0)
    //  val dev = userItemDevs(uId, iId)
    //  val scaling = scale(dev + uAverage, uAverage)
    //  predictionsBuilder.add(uId, iId, uAverage + dev * scaling)
    //  }
    //})
    //
    //val predictions = predictionsBuilder.result()
  //
    //return ((uId : UserId, iId : ItemId) => {
    //  val out = predictions(uId, iId)
    //  if(userCounts(uId, 0) == 0){
    //    globalAverage
    //  } else {
    //    out
    //  }
    //}, topKSims)


    return ((uId : UserId, iId : ItemId) => predictKNN(uId, iId, train = train, topKSims = topKSims, usersAverage =  usersAverage, devs= devs, globalAverage ), topKSims)

  }


  // END OF PART 1


  // PART 2 ///////////////////////////////////////////

    //def fromSliceToDense(slice: SliceVector[(Int, Int), Double]): DenseVector[Double] = {
    //    val dense = DenseVector.zeros[Double](slice.length)
    //    slice.activeIterator.foreach({ case (i,v) => dense(i) = v})
    //    return dense
    //}


    def computeExactSparkSimilarities(preprocRatings: CSCMatrix[Double], k:Int, sc: SparkContext): CSCMatrix[Double]  = {
    val broadcastPreprocRatings = sc.broadcast(preprocRatings)

    val nbUsers = preprocRatings.rows
    val nbItems = preprocRatings.cols

    def topK(user: Int): SparseVector[Double] = {

      val preProc = broadcastPreprocRatings.value
      val userSlicePreProc = preProc(user, 0 until nbItems).t.toDenseVector
      val userSims = preProc * userSlicePreProc
      userSims(user) = 0.0
      val topSims = SparseVector.zeros[Double](nbUsers)
      for(v <- argtopk(userSims,k)){
        topSims(v) = userSims(v)
      }

      return topSims
  }
    
      val topKs = sc.parallelize(0 until nbUsers).map(u => topK(u)).collect()
      // freeing memory
      // broadcastPreprocRatings.destroy()
      val topKSimsBuilder = new CSCMatrix.Builder[Double](nbUsers, nbUsers) 
      
      for(u <- 0 until nbUsers){
        val topSims = topKs(u)

        for((v, suv) <- topSims.activeIterator){

          topKSimsBuilder.add(u,v,suv)
        }
      }

      return topKSimsBuilder.result()
    }

  def SparkKNNPredictor(train : RateMatrix, k : Int, sc:SparkContext) : (Predictor, RateMatrix) = { 

    val (usersAverage, globalAverage) = globalAverageAndUsersAverage(train)
    val (preprocRatings,devs) = preprocessRatings(train,usersAverage)
    val topKSims = computeExactSparkSimilarities(preprocRatings,k,sc)

    return ((uId : UserId, iId : ItemId) => predictKNN(uId, iId, train = train, topKSims = topKSims, usersAverage =  usersAverage, devs= devs, globalAverage ), topKSims)

  }

    // END OF PART 2 ///////////////////////////////////////////

  // PART 3 ///////////////////////////////////////////

  def usersToPartitions(nbUsers:Int, partitionedUsers: Seq[Set[Int]]): Map[Int,Set[Int]] = {

    val userToPartitions = collection.mutable.Map[Int, Set[Int]]()

    for(user <- 0 until nbUsers){

      val partitions = Set.newBuilder[Int]
      for( i <- 0 until partitionedUsers.length){

        if(partitionedUsers(i).contains(user)){
          partitions += i
        }
      }

      userToPartitions += (user -> partitions.result())
  }

    return userToPartitions.toMap
  }

  def preprocessRatingsPartitioned(train: CSCMatrix[Double],usersAverage: CSCMatrix[Double], partitionedUsers: Seq[Set[Int]]): (List[CSCMatrix[Double]], CSCMatrix[Double]) = {
  
    
    val nbUsers = train.rows
    val nbItems = train.cols
    val nbPartitions = partitionedUsers.length


        // compute the deviations r(u, i)^ = (r(u, i)  - ravg(u)) / scale(r(u, i), ravg(u))
    var preScaleBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)

    train.activeIterator.foreach({case ((uId, iId), r) => {
      val uAverage = usersAverage(uId, 0)
      val scaling = scale(r, uAverage)
      preScaleBuilder.add(uId, iId, (r - uAverage) / scaling.toDouble)

    }})
    val devs = preScaleBuilder.result()
      // compute the preprocessed ratings r~(u, i) = r(u, i)^ / sqrt(sum(all the items rated by one user r^(u, j) squared ))
    //val norms = sqrt(sumAlongAxis(devs :* devs, axis=1))
    val norms = sqrt((devs *:* devs) * DenseVector.ones[Double](devs.cols))




    val listPreprocRatings = (0 until nbPartitions).map(x => new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)).toList

    val userToPartitions = usersToPartitions(nbUsers, partitionedUsers)
    
    devs.activeIterator.foreach({case ((uId, iId), r) => {

      // filling the preprocratings only with the assigned users for each partitions
      for(partitionNumber <-  userToPartitions(uId)){

        listPreprocRatings(partitionNumber).add(uId, iId, r / norms(uId))

      }
    }})
    

    return (listPreprocRatings.map(builder => builder.result()), devs)
  }

  def keepTopKInList(listOfSims:List[(Int,Double)], k:Int): List[(Int,Double)] = {

    // use negative sim to sort by ascending order
    return listOfSims.sortBy({case (vId, sim) => -sim}).take(k)
  }

  def computeApproximateSimilarities(listPreprocRatings : List[CSCMatrix[Double]], k:Int): CSCMatrix[Double] = {

        val nbUsers = listPreprocRatings(0).rows

        val topKSimsBuilder = new CSCMatrix.Builder[Double](nbUsers, nbUsers) 

        val userTopKSimsMap =  collection.mutable.Map[Int, List[(Int,Double)]]()


        for( preprocRatings <- listPreprocRatings){
        
        val sims = preprocRatings * preprocRatings.t

        // settings similarity to zero
        for (uId <- 0 until nbUsers){
          sims(uId,uId) = 0.0
        }

        // keep only the topKSims for each users
        val tops = argtopk(sims.toDense(::, *), k)

        for (uId <- 0 until nbUsers){
            // concatenate current top K simimlarities with the new K similarities and keeping the best
            val newTopKSims = tops(uId).map(vId => (vId, sims(uId,vId))).toList
            val currentTopKSims = userTopKSimsMap.getOrElse(uId,List.empty[(Int,Double)])

            userTopKSimsMap(uId) = keepTopKInList( currentTopKSims ::: newTopKSims , k)
          }

        }

        for (uId <- 0 until nbUsers){
            for((vId, sim) <- userTopKSimsMap(uId)){
              topKSimsBuilder.add(uId,vId,sim)
            }
        }

        return topKSimsBuilder.result()
  } 



  def ApproximateKNNPredictor(train : RateMatrix, k : Int, partitionedUsers : Seq[Set[Int]]) : (Predictor, RateMatrix) = { 

    val (usersAverage, globalAverage) = globalAverageAndUsersAverage(train)
    val (listPreprocRatings,devs) = preprocessRatingsPartitioned(train,usersAverage, partitionedUsers)
    val topKSims = computeApproximateSimilarities(listPreprocRatings, k)

    return ((uId : UserId, iId : ItemId) => predictKNN(uId, iId, train = train, topKSims = topKSims, usersAverage =  usersAverage, devs= devs, globalAverage ), topKSims)

  }


/// SPARK FOR PART 3 ///

  def preProcessRatingsForPartition(train:CSCMatrix[Double], partition:Set[Int], usersAverage:CSCMatrix[Double]): (CSCMatrix[Double],CSCMatrix[Double]) = {
  
    val nbUsers = train.rows
    val nbItems = train.cols


        // compute the deviations r(u, i)^ = (r(u, i)  - ravg(u)) / scale(r(u, i), ravg(u))
    var preScaleBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)

    train.activeIterator.foreach({case ((uId, iId), r) => {

      if(partition.contains(uId)){
        
      val uAverage = usersAverage(uId, 0)
      val scaling = scale(r, uAverage)
      val dev = (r - uAverage) / scaling.toDouble
        if(iId == 81 && uId == 750){
          println(s"uid $uId iid $iId dev $dev ")
        }
        
      preScaleBuilder.add(uId, iId, dev)
      }

      }
    })


    val devs = preScaleBuilder.result()
      // compute the preprocessed ratings r~(u, i) = r(u, i)^ / sqrt(sum(all the items rated by one user r^(u, j) squared ))
    //val norms = sqrt(sumAlongAxis(devs :* devs, axis=1))
    val norms = sqrt((devs *:* devs) * DenseVector.ones[Double](devs.cols))

    val preprocRatingsBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)

    
    devs.activeIterator.foreach({case ((uId, iId), r) => {
      // filling the preprocratings only with the assigned users for each partitions
      if(partition.contains(uId)){

        preprocRatingsBuilder.add(uId, iId, r / norms(uId))

      }
    }})
    

    return (preprocRatingsBuilder.result(), devs)
    
  } 


  def computeApproximateSparkSimilaritiesAndDevs(train: CSCMatrix[Double], k:Int, sc:SparkContext , partitionedUsers: Seq[Set[Int]], usersAverage: CSCMatrix[Double]): (CSCMatrix[Double],CSCMatrix[Double]) = {

      val nbPartitions = partitionedUsers.length
      val nbUsers = train.rows
      val nbItems = train.cols

      val broadcastPartitions = sc.broadcast(partitionedUsers)


    // returns the list containing for each user the top K similarities for this given partition
    def topKSimsForPartitionAndDevs(partitionNumber:Int): (List[List[(Int,Double)]], CSCMatrix[Double]) = {

      val currPartition = broadcastPartitions.value(partitionNumber)
      val (preprocRatings, devs) = preProcessRatingsForPartition(train, currPartition, usersAverage = usersAverage)

      val sims = preprocRatings * preprocRatings.t

        // settings similarity to zero
        for (uId <- 0 until nbUsers){
          sims(uId,uId) = 0.0
        }

      val tops = argtopk(sims.toDense(::, *), k)

      val topKSims = List.newBuilder[List[(Int,Double)]]

      for (uId <- 0 until nbUsers){
            // concatenate current top K simimlarities with the new K similarities and keeping the best
            topKSims += tops(uId).map(vId => (vId, sims(uId,vId))).toList

          }

          return (topKSims.result(),devs)

    }
    
      val listOfTopKsAndDevs = sc.parallelize(0 until nbPartitions).map(partitionNumber => topKSimsForPartitionAndDevs(partitionNumber)).collect()

      val userTopKSimsMap =  collection.mutable.Map[Int, List[(Int,Double)]]()

      // quickly building the final devs because we need them for the lazy evaluation later
      val finalDevs = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)

      for(partitionNumber <- 0 until nbPartitions){

        for(((uId, iId), dev) <- listOfTopKsAndDevs(partitionNumber)._2.activeIterator ){
            finalDevs.add(uId,iId,dev)
        }
      
      }


      // building the topKSims
            
      for(partitionNumber <- 0 until nbPartitions){
        val partitionTopKSims = listOfTopKsAndDevs(partitionNumber)._1

        for (uId <- 0 until nbUsers){
            // concatenate current top K simimlarities with the new K similarities and keeping the best
            val newTopKSims = partitionTopKSims(uId)
            val currentTopKSims = userTopKSimsMap.getOrElse(uId,List.empty[(Int,Double)])

            userTopKSimsMap(uId) = keepTopKInList( currentTopKSims ::: newTopKSims , k)
          }
        }


      val topKSimsBuilder = new CSCMatrix.Builder[Double](nbUsers, nbUsers) 

      for(u <- 0 until nbUsers){
        val topSims = userTopKSimsMap(u)

        for((v, suv) <- topSims){

          topKSimsBuilder.add(u,v,suv)
        }
      }

      return (topKSimsBuilder.result(),finalDevs.result())
  }


  def ApproximateKNNSparkPredictor(train : RateMatrix, k : Int, partitionedUsers : Seq[Set[Int]], sc: SparkContext): (Predictor, RateMatrix) = { 

    val (usersAverage, globalAverage) = globalAverageAndUsersAverage(train)
    val (topKSims,devs) = computeApproximateSparkSimilaritiesAndDevs(train, k, sc , partitionedUsers, usersAverage = usersAverage)

    println(devs(726,81))
    println(devs(750,81))

    return ((uId : UserId, iId : ItemId) => predictKNN(uId, iId, train = train, topKSims = topKSims, usersAverage =  usersAverage, devs= devs, globalAverage ), topKSims)

  }

  // END OF PART 3 ///////////////////////////////////////////
    

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
    val ratings = loadSparkRDD(sc, path,sep).collect()

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


  def loadSparkRDD(sc : org.apache.spark.SparkContext, path : String, sep : String): RDD[((Int,Int),Double)] = {


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
             case None => ((-1, -1), -1.0) })

      
        return ratings

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


