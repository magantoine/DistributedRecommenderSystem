package shared
import scala.collection.mutable
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.In

package object predictions {
  // ------------------------ For template
  case class Rating(user: Int, item: Int, rating: Double)

  type Dataset = List[Rating]
  type RateMatrix = CSCMatrix[Rate]
  type UserId = Int
  type ItemId = Int
  type Rate = Double
  type Predictor = (UserId, ItemId) => Rate

// PART 1 BREEZE

  def numberOfUser(data: Dataset): Int = {
    return data.map(_.user).toSet.size
  }

  def numberOfItems(data: Dataset): Int = {
    return data.map(_.item).toSet.size
  }

  def shape(m: RateMatrix): (Int, Int) = {
    return (m.rows, m.cols)
  }

  def sumAlongAxis(m: RateMatrix, axis: Int): CSCMatrix[Double] = {
    var multiplicator =
      if (axis == 1) new CSCMatrix.Builder[Double](rows = m.cols, cols = 1)
      else new CSCMatrix.Builder[Double](rows = 1, cols = m.rows)
    if (axis == 1) {
      for (i <- 0 until m.cols) {
        multiplicator.add(i, 0, 1)
      }
    } else {
      for (i <- 0 until m.rows) {
        multiplicator.add(0, i, 1)
      }
    }

    return if (axis == 1) dot(m, multiplicator.result())
    else dot(multiplicator.result(), m)
  }

  def computeMAE(test: RateMatrix, predictor: Predictor): Double = {
    var mae = 0.0
    var cnt = 0
    test.activeIterator.foreach({
      case ((uId, iId), r) => {
        mae += abs(r - predictor(uId, iId))
        cnt = cnt + 1
      }
    })

    return mae / cnt.toDouble
  }

  def scale(userRating: Double, userAvgRating: Double): Double =
    userRating match {
      case x if x > userAvgRating  => 5.0 - userAvgRating
      case x if x < userAvgRating  => userAvgRating - 1.0
      case x if x == userAvgRating => 1.0
    }

  def mmReducer(
      productElements: ((Int, Int), Iterable[((Int, Int), Double)])
  ): (Int, Int, Double) = {
    val ((i, j), elems) = (productElements._1, productElements._2.toList)

    val count = elems
      .groupBy(_._1._2)
      .filter({ case (idComputation, items) => items.toList.length == 2 })
    return (
      i,
      j,
      count.foldLeft(0.0)({ case (acc, (idComputation, items)) =>
        acc + items.toList(0)._2 * items.toList(1)._2
      })
    )
  }

  def mmMapper(
      N: Int,
      M: Int,
      index: ((Int, Int), Double),
      matIndex: Int
  ): List[((Int, Int), (Int, Int), Double)] = {
    // for each index of the final matrix we want to find the list of index
    // we need to make the multiplication.

    // href : https://lendap.wordpress.com/2015/02/16/matrix-multiplication-with-mapreduce/

    val ((i, j), value) = index // index we want obtain

    if (matIndex == 0) {
      return ((0 until N) map { k => ((k, j), (0, i), value) }).toList
    } else {
      return ((0 until M) map { k => ((i, k), (1, j), value) }).toList
    }
  }

  // def dot(a : RateMatrix, b : RateMatrix) : RateMatrix = {
  //   val matrix = (b.activeIterator.flatMap(mmMapper(a.rows, b.cols, _, 0))) ++ (a.activeIterator.flatMap(mmMapper(a.rows, b.cols, _, 1)))
  //   val matBuilder = new CSCMatrix.Builder[Rate](rows=a.rows, cols=b.cols)
  //   matrix.toList
  //     .groupBy(_._1)
  //     .map({case ((x, y), ws) => mmReducer(((x, y), ws.map(x => (x._2, x._3))))})
  //     .filter(_._3 != 0)
  //     .foreach({case (x, y, z) => matBuilder.add(x, y, z)})
  //   return matBuilder.result()

  // }

  // def dot(a : RateMatrix, b : RateMatrix) : RateMatrix = {
  //   return a * b
  // }
  /** Does a @ b
    */
  // def dot(a : RateMatrix, b : RateMatrix) : RateMatrix = {
  //   assert(a.cols == b.rows)
  //   var builder = new CSCMatrix.Builder[Rate](a.rows, b.cols)
  //   for (u <- 0 until a.rows; v <- 0 until b.cols) {
  //     // var value = 0.0
  //     //value = a(u, 0 until a.cols) * b(0 until b.rows, v)
  //     var ind = a.t(0 until a.cols, u).activeIterator.toList
  //     ind = ind ++ b(0 until b.rows, v).activeIterator.toList.map({case ((x, y), z) => ((y, x), z)})
  //     var value = ind.groupBy(x => x._1).filter({case (x, ws) => ws.length == 2}).map({case (x, ws) => ws(0)._2 * ws(1)._2}).reduce(_+_)
  //     //var value = a.t(0 until a.cols, u).activeIterator.toList.(b(0 until b.rows, v).activeIterator.toList).foldLeft(0.0)({case (acc, (valA, valB)) => acc + valA._2 * valB._2})
  //     if(value != 0){
  //         builder.add(u, v, value)
  //     }
  //     // a.t(0 until a.cols, u).foreach(ui => {
  //     //   b(0 until b.rows, v).foreach(vi => {
  //     //     value += ui * vi
  //     //     if(value != 0){
  //     //     builder.add(u, v, value)
  //     //     }
  //     //   })
  //     // })
  //   }
  //   return builder.result()

  // }

  def dot(a: RateMatrix, b: RateMatrix): RateMatrix = {
    assert(a.cols == b.rows)

    // if(a.cols < 1000 && a.rows < 1000 && b.cols < 1000){
    //   return a * b
    // }
    var acc = new CSCMatrix.Builder[Rate](a.rows, b.cols).result()

    for (id <- 0 until a.cols) {
      // we iterate through all the columns
      var vecA = SparseVector(a(0 until a.rows, id).toArray).asCSCMatrix
      var vecB = SparseVector(b(id, 0 until b.cols).t.toArray).asCSCMatrix
      acc = acc + vecA.t * vecB

    }

    return acc
  }

  def globalAverageAndUsersAverage(
      train: CSCMatrix[Double]
  ): (CSCMatrix[Double], Double) = {
    val nbUsers = train.rows
    val nbItems = train.cols
    var maskBuilder =
      new CSCMatrix.Builder[Rate](rows = nbUsers, cols = nbItems)

    var cnt = 0.0
    var sumOfRate = 0.0

    train.activeIterator.foreach({
      case ((uId, iId), r) => {
        maskBuilder.add(uId, iId, 1)
        cnt += 1
        sumOfRate += r
      }
    })

    val globalAverage = sumOfRate / cnt

    val mask = maskBuilder.result()

    val userCounts = sumAlongAxis(mask, axis = 1)

    // compute average rating per user
    val usersAverage = sumAlongAxis(train, axis = 1) /:/ userCounts

    return (usersAverage, globalAverage)
  }

  def computeNormalizedDeviations(
      train: CSCMatrix[Double],
      usersAverage: CSCMatrix[Double]
  ): CSCMatrix[Double] = {
    val nbUsers = train.rows
    val nbItems = train.cols

    // compute the deviations r(u, i)^ = (r(u, i)  - ravg(u)) / scale(r(u, i), ravg(u))
    var preScaleBuilder =
      new CSCMatrix.Builder[Rate](rows = nbUsers, cols = nbItems)

    train.activeIterator.foreach({
      case ((uId, iId), r) => {
        val uAverage = usersAverage(uId, 0)
        val scaling = scale(r, uAverage)
        preScaleBuilder.add(uId, iId, (r - uAverage) / scaling.toDouble)

      }
    })
    val devs = preScaleBuilder.result()

    return devs
  }

  def preprocessRatings(
      train: CSCMatrix[Double],
      usersAverage: CSCMatrix[Double]
  ): (CSCMatrix[Double], CSCMatrix[Double]) = {

    val nbUsers = train.rows
    val nbItems = train.cols

    val devs = computeNormalizedDeviations(train, usersAverage)

    // compute the preprocessed ratings r~(u, i) = r(u, i)^ / sqrt(sum(all the items rated by one user r^(u, j) squared ))
    // val norms = sqrt(sumAlongAxis(devs :* devs, axis=1))
    val norms = sqrt((devs *:* devs) * DenseVector.ones[Double](devs.cols))

    var preprocRatingBuilder =
      new CSCMatrix.Builder[Rate](rows = nbUsers, cols = nbItems)
    devs.activeIterator.foreach({
      case ((uId, iId), r) => {
        preprocRatingBuilder.add(uId, iId, r / norms(uId))
      }
    })

    val preprocRatings = preprocRatingBuilder.result()

    return (preprocRatings, devs)
  }

  def computeKTopSimilarites(
      preprocRatings: CSCMatrix[Double],
      k: Int
  ): CSCMatrix[Double] = {

    val nbUsers = preprocRatings.rows
    val nbItems = preprocRatings.cols
    // compute the similarities, sum for all items j rated by both r~(u, j) * r~(v, j)
    println("computing sims")

    // TESTING SOMETHIING HERE //
    
    
    val sims = if(nbUsers < 1000) preprocRatings * preprocRatings.t else dot(preprocRatings, preprocRatings.t)
    // val simsBuildos = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbUsers)
    // // for(userPair <- (0 until nbUsers).toList.combinations(2).toList){
    // //   //val value = sumAlongAxis(preprocRatings(userPair, 0 until nbItems), axis=0).activeValuesIterator.sum
    // //   val value = (preprocRatings(userPair(0), 0 until nbItems) :* preprocRatings(userPair(1), 0 until nbItems)).t.sum
      
    // //   simsBuildos.add(userPair(0), userPair(1), value)
    // //   simsBuildos.add(userPair(1), userPair(0), value)
    // // }
      


    
    


    // keep only the topKSims for each users
    var topKSimsBuilder =
      new CSCMatrix.Builder[Rate](rows = nbUsers, cols = nbUsers)
    println("computing topK")
    // val tops = argtopk(sims.toDense(::, *), k + 1)
    val tops = (0 until nbUsers).map(u => {
        sims(0 until nbUsers, u).toDenseVector.argtopk(k + 1)})

    println("filling topK")
    for (uId <- 0 until nbUsers) {
      for (vId <- tops(uId)) {
        if (uId != vId) {
          topKSimsBuilder.add(uId, vId, sims(uId, vId))
        }
      }
    }

    val topKSims = topKSimsBuilder.result()

    return topKSims
  }

  def userItemDeviations(
      train: CSCMatrix[Double],
      topKSims: CSCMatrix[Double],
      devs: CSCMatrix[Double]
  ): CSCMatrix[Double] = {

    val nbUsers = train.rows
    val nbItems = train.cols

    var maskBuilder =
      new CSCMatrix.Builder[Rate](rows = nbUsers, cols = nbItems)

    train.activeIterator.foreach({
      case ((uId, iId), r) => {
        maskBuilder.add(uId, iId, 1)
      }
    })

    val mask = maskBuilder.result()

    val denoms = dot(abs(topKSims), mask)
    val nums = dot(topKSims, devs)

    var userItemDevBuilder =
      new CSCMatrix.Builder[Rate](rows = nbUsers, cols = nbItems)
    nums.activeIterator.foreach({
      case ((uId, iId), num) => {
        userItemDevBuilder.add(uId, iId, num / denoms(uId, iId).toDouble)
      }
    })

    val userItemDevs = userItemDevBuilder.result()

    return userItemDevs
  }

  def computeUserItemAverageDev(
      user: Int,
      item: Int,
      train: CSCMatrix[Double],
      topKSims: CSCMatrix[Double],
      devs: CSCMatrix[Double]
  ): Double = {

    val nbUsers = train.rows
    val nbItems = train.cols

    val itemRatings = train(0 until nbUsers, item).toDenseVector.map(x =>
      if (x != 0.0) 1.0 else 0.0
    )

    val denom =
      itemRatings.t * abs(topKSims(user, 0 until nbUsers).t.toDenseVector)
    val num = topKSims(user, 0 until nbUsers) * devs(0 until nbUsers, item)

    val res = if (denom != 0.0) num / denom else 0.0

    return res
  }

  def predictKNN(
      user: Int,
      item: Int,
      train: CSCMatrix[Double],
      topKSims: CSCMatrix[Double],
      usersAverage: CSCMatrix[Double],
      devs: CSCMatrix[Double],
      globalAverage: Double
  ): Double = {

    val nbUsers = train.rows
    val userItemAverageDev =
      computeUserItemAverageDev(user, item, train, topKSims, devs)
    val itemRatings = train(0 until nbUsers, item).toDenseVector.map(x =>
      if (x != 0.0) 1.0 else 0.0
    )
    val num_item_ratings = sum(itemRatings)
    val userAverage = usersAverage(user, 0)

    if (userItemAverageDev == 0.0 || num_item_ratings == 0.0) return userAverage
    else {
      val num_user_sims = sum(
        topKSims(user, 0 until nbUsers).t.toDenseVector.map(x =>
          if (x != 0.0) 1.0 else 0.0
        )
      )
      if (num_user_sims == 0.0) { return globalAverage }
      else {
        // println(s"User $user Item $item userAverage $userAverage userItemAverageDev  $userItemAverageDev num_item_ratings  $num_item_ratings")
        return userAverage + userItemAverageDev * scale(
          userAverage + userItemAverageDev,
          userAverage
        )
      }
    }
  }

  def kNNPredictor(train: RateMatrix, k: Int): (Predictor, RateMatrix) = {

    println("computing global average")
    val (usersAverage, globalAverage) = globalAverageAndUsersAverage(train)
    println("computing preprocess ratings")
    val (preprocRatings, devs) = preprocessRatings(train, usersAverage)
    println("computing top k sims")
    val topKSims = computeKTopSimilarites(preprocRatings, k)

    // val userItemDevs = userItemDeviations(train = train, devs = devs, topKSims =  topKSims)
    // val userItemPairs = for(uId <- 0 until train.rows; iId <- 0 until train.cols) yield (uId, iId)
    // var predictionsBuilder = new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)
    // userItemPairs.foreach({ case (uId, iId) => {
    //  val uAverage = usersAverage(uId, 0)
    //  val dev = userItemDevs(uId, iId)
    //  val scaling = scale(dev + uAverage, uAverage)
    //  predictionsBuilder.add(uId, iId, uAverage + dev * scaling)
    //  }
    // })
    //
    // val predictions = predictionsBuilder.result()
    //
    // return ((uId : UserId, iId : ItemId) => {
    //  val out = predictions(uId, iId)
    //  if(userCounts(uId, 0) == 0){
    //    globalAverage
    //  } else {
    //    out
    //  }
    // }, topKSims)

    return (
      (uId: UserId, iId: ItemId) =>
        predictKNN(
          uId,
          iId,
          train = train,
          topKSims = topKSims,
          usersAverage = usersAverage,
          devs = devs,
          globalAverage
        ),
      topKSims
    )

  }

  // END OF PART 1

  // PART 2 ///////////////////////////////////////////

  // def fromSliceToDense(slice: SliceVector[(Int, Int), Double]): DenseVector[Double] = {
  //    val dense = DenseVector.zeros[Double](slice.length)
  //    slice.activeIterator.foreach({ case (i,v) => dense(i) = v})
  //    return dense
  // }

  def computeExactSparkSimilarities(
      preprocRatings: CSCMatrix[Double],
      k: Int,
      sc: SparkContext
  ): CSCMatrix[Double] = {
    val broadcastPreprocRatings = sc.broadcast(preprocRatings)

    val nbUsers = preprocRatings.rows
    val nbItems = preprocRatings.cols

    def topK(user: Int): SparseVector[Double] = {

      val preProc = broadcastPreprocRatings.value
      val userSlicePreProc = preProc(user, 0 until nbItems).t.toDenseVector
      val userSims = preProc * userSlicePreProc
      userSims(user) = 0.0
      val topSims = SparseVector.zeros[Double](nbUsers)
      for (v <- argtopk(userSims, k)) {
        topSims(v) = userSims(v)
      }

      return topSims
    }

    val topKs = sc.parallelize(0 until nbUsers).map(u => topK(u)).collect()
    // freeing memory
    // broadcastPreprocRatings.destroy()
    val topKSimsBuilder = new CSCMatrix.Builder[Double](nbUsers, nbUsers)

    for (u <- 0 until nbUsers) {
      val topSims = topKs(u)

      for ((v, suv) <- topSims.activeIterator) {

        topKSimsBuilder.add(u, v, suv)
      }
    }

    return topKSimsBuilder.result()
  }

  def SparkKNNPredictor(
      train: RateMatrix,
      k: Int,
      sc: SparkContext
  ): (Predictor, RateMatrix) = {

    val (usersAverage, globalAverage) = globalAverageAndUsersAverage(train)
    val (preprocRatings, devs) = preprocessRatings(train, usersAverage)
    val topKSims = computeExactSparkSimilarities(preprocRatings, k, sc)

    return (
      (uId: UserId, iId: ItemId) =>
        predictKNN(
          uId,
          iId,
          train = train,
          topKSims = topKSims,
          usersAverage = usersAverage,
          devs = devs,
          globalAverage
        ),
      topKSims
    )

  }

  // END OF PART 2 ///////////////////////////////////////////

  // PART 3 ///////////////////////////////////////////
//
  // def usersToPartitions(nbUsers:Int, partitionedUsers: Seq[Set[Int]]): Map[Int,Set[Int]] = {
//
  //  val userToPartitions = collection.mutable.Map[Int, Set[Int]]()
//
  //  for(user <- 0 until nbUsers){
//
  //    val partitions = Set.newBuilder[Int]
  //    for( i <- 0 until partitionedUsers.length){
//
  //      if(partitionedUsers(i).contains(user)){
  //        partitions += i
  //      }
  //    }
//
  //    userToPartitions += (user -> partitions.result())
  // }
//
  //  return userToPartitions.toMap
  // }
//
  // def preprocessRatingsPartitioned(train: CSCMatrix[Double],usersAverage: CSCMatrix[Double], partitionedUsers: Seq[Set[Int]]): (List[CSCMatrix[Double]], CSCMatrix[Double]) = {
  //
  //
  //  val nbUsers = train.rows
  //  val nbItems = train.cols
  //  val nbPartitions = partitionedUsers.length
//
//
  //  val devs = computeNormalizedDeviations(train,usersAverage)
  //    // compute the preprocessed ratings r~(u, i) = r(u, i)^ / sqrt(sum(all the items rated by one user r^(u, j) squared ))
  //  //val norms = sqrt(sumAlongAxis(devs :* devs, axis=1))
  //  val norms = sqrt((devs *:* devs) * DenseVector.ones[Double](devs.cols))
//
//
//
//
  //  val listPreprocRatings = (0 until nbPartitions).map(x => new CSCMatrix.Builder[Rate](rows=nbUsers, cols=nbItems)).toList
//
  //  val userToPartitions = usersToPartitions(nbUsers, partitionedUsers)
  //  print(userToPartitions)
  //  /* MIGHT BE HERE, TRY GIVING THE */
  //  devs.activeIterator.foreach({case ((uId, iId), r) => {
//
  //    // filling the preprocratings only with the assigned users for each partitions
  //    for(partitionNumber <-  userToPartitions(uId)){
//
  //      listPreprocRatings(partitionNumber).add(uId, iId, r / norms(uId))
//
  //    }
  //  }})
  //
//
  //  return (listPreprocRatings.map(builder => builder.result()), devs)
  // }
//
  def keepTopKInList(
      listOfSims1: List[(Int, Double)],
      listOfSims2: List[(Int, Double)],
      k: Int
  ): List[(Int, Double)] = {

    // use negative sim to sort by ascending order

    val listOfSims = listOfSims1 ::: listOfSims2

    return listOfSims
      .groupBy(_._2)
      .map(_._2.head)
      .toList
      .sortBy({ case (vId, sim) => -sim })
      .take(k)
  }

/// SPARK FOR PART 3 ///

  def preProcessRatingsForPartition(
      train: CSCMatrix[Double],
      partition: Set[Int],
      usersAverage: CSCMatrix[Double],
      devs: CSCMatrix[Double]
  ): CSCMatrix[Double] = {

    val nbUsers = train.rows
    val nbItems = train.cols

    // compute the preprocessed ratings r~(u, i) = r(u, i)^ / sqrt(sum(all the items rated by one user r^(u, j) squared ))
    // val norms = sqrt(sumAlongAxis(devs :* devs, axis=1))
    val norms = sqrt((devs *:* devs) * DenseVector.ones[Double](devs.cols))

    val preprocRatingsBuilder =
      new CSCMatrix.Builder[Rate](rows = nbUsers, cols = nbItems)

    devs.activeIterator.foreach({
      case ((uId, iId), r) => {
        // filling the preprocratings only with the assigned users for each partitions
        if (partition.contains(uId)) {

          preprocRatingsBuilder.add(uId, iId, r / norms(uId))

        }
      }
    })

    return preprocRatingsBuilder.result()

  }

  def computeApproximateSparkSimilarities(
      train: CSCMatrix[Double],
      k: Int,
      sc: SparkContext,
      partitionedUsers: Seq[Set[Int]],
      usersAverage: CSCMatrix[Double]
  ): (CSCMatrix[Double], CSCMatrix[Double]) = {

    val nbPartitions = partitionedUsers.length
    val nbUsers = train.rows
    val nbItems = train.cols

    // broadcasting important variables
    val broadcastPartitions = sc.broadcast(partitionedUsers)
    val devs = computeNormalizedDeviations(train, usersAverage)
    val broadcastDevs = sc.broadcast(devs)

    // returns the list containing for each user the top K similarities for this given partition
    // SPARK PROCEDURE
    def topKSimsForPartitionAndDevs(
        partitionNumber: Int
    ): List[(UserId, List[(UserId, Double)])] = {

      val currPartition = broadcastPartitions.value(partitionNumber).toList.sorted
      print(s"for partition ${partitionNumber} we have :")
      val partToRealIndices = currPartition.zipWithIndex.map({case (realIndex, partIndex) => (partIndex -> realIndex)}).toMap
      val realToPartIndices = currPartition.zipWithIndex.map({case (realIndex, partIndex) => (realIndex -> partIndex)}).toMap

      println(s"getting preprocRatings ${partitionNumber}")
      // val preprocRatings = preprocessRatings(train,usersAverage)
      val preprocRatings = preProcessRatingsForPartition(
        train,
        currPartition.toSet,
        usersAverage = usersAverage,
        devs = broadcastDevs.value
      )

      // we build reduced preprocRatings Matrix
      val reducedBuilder = new CSCMatrix.Builder[Rate](rows=currPartition.length, nbItems)
      preprocRatings.activeIterator.foreach({ case ((uId, iId), v) => {
        reducedBuilder.add(realToPartIndices(uId), iId, v)
      }})

      val reduced = reducedBuilder.result()


      println(s"computing sims for partitions ${partitionNumber}")
      val sims = dot(reduced, reduced.t)
      

      // // settings similarity to zero
      // val expandedSims = expandedBuilder.result()
      // println(s"getting topk for part ${partitionNumber}")
      //val tops = argtopk(sims.toDense(::, *), min(k + 1, currPartition.length))
      
      val tops = currPartition.map(u => {
        sims(0 until currPartition.length, realToPartIndices(u)).toDenseVector.argtopk(min(k + 1, currPartition.length))})

      var topKSims = mutable.Map[UserId, List[(UserId, Double)]]()

      println(s" building final list for ${partitionNumber}")
      // DO NOT ITERATE THROUGH THE WHOLE USERS BUT JUST THE PARTITION
      for (uId <- currPartition) {
        // concatenate current top K simimlarities with the new K similarities and keeping the best
        topKSims += (uId -> tops(realToPartIndices(uId))
          .filter(_ != realToPartIndices(uId))
          .map(vId => (partToRealIndices(vId), sims(realToPartIndices(uId), vId)))
          .toList)
      }

      return topKSims.toList

    }

    println(s">> merging <<")
    val listOfTopKs = sc.parallelize(0 until nbPartitions)
      .flatMap(partitionNumber => topKSimsForPartitionAndDevs(partitionNumber))
      .reduceByKey({ case (listOfSims1, listOfSims2) =>
        keepTopKInList(listOfSims1, listOfSims2, k)
      }).collect()
    

    val topKSimsBuilder = new CSCMatrix.Builder[Double](nbUsers, nbUsers)

    listOfTopKs.foreach({ case (uId, listOfSims) => {
      for ((v, suv) <- listOfSims) {

        topKSimsBuilder.add(uId, v, suv)
       }
      }
    })
    // for ((uId, listOfSims) <- listOfTopKs) {

    //   for ((v, suv) <- listOfSims) {

    //     topKSimsBuilder.add(uId, v, suv)
    //   }
    // }

    return (topKSimsBuilder.result(), devs)
  }

  def ApproximateKNNSparkPredictor(
      train: RateMatrix,
      k: Int,
      partitionedUsers: Seq[Set[Int]],
      sc: SparkContext
  ): (Predictor, RateMatrix) = {

    val (usersAverage, globalAverage) = globalAverageAndUsersAverage(train)
    val (topKSims, devs) = computeApproximateSparkSimilarities(
      train,
      k,
      sc,
      partitionedUsers,
      usersAverage = usersAverage
    )

    return (
      (uId: UserId, iId: ItemId) =>
        predictKNN(
          uId,
          iId,
          train = train,
          topKSims = topKSims,
          usersAverage = usersAverage,
          devs = devs,
          globalAverage
        ),
      topKSims
    )

  }

  // END OF PART 3 ///////////////////////////////////////////

  def timingInMs(f: () => Double): (Double, Double) = {
    val start = System.nanoTime()
    val output = f()
    val end = System.nanoTime()
    return (output, (end - start) / 1000000.0)
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def mean(s: Seq[Double]): Double =
    if (s.size > 0) s.reduce(_ + _) / s.length else 0.0

  def std(s: Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      scala.math.sqrt(
        s.map(x => scala.math.pow(m - x, 2)).sum / s.length.toDouble
      )
    }
  }

  def load(
      path: String,
      sep: String,
      nbUsers: Int,
      nbMovies: Int
  ): CSCMatrix[Double] = {
    val file = Source.fromFile(path)
    val builder = new CSCMatrix.Builder[Double](rows = nbUsers, cols = nbMovies)
    for (line <- file.getLines) {
      val cols = line.split(sep).map(_.trim)
      toInt(cols(0)) match {
        case Some(_) =>
          builder.add(cols(0).toInt - 1, cols(1).toInt - 1, cols(2).toDouble)
        case None => None
      }
    }
    file.close
    builder.result()
  }

  def loadSpark(
      sc: org.apache.spark.SparkContext,
      path: String,
      sep: String,
      nbUsers: Int,
      nbMovies: Int
  ): CSCMatrix[Double] = {
    val ratings = loadSparkRDD(sc, path, sep).collect()

    val builder = new CSCMatrix.Builder[Double](rows = nbUsers, cols = nbMovies)
    for ((k, v) <- ratings) {
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

  def loadSparkRDD(
      sc: org.apache.spark.SparkContext,
      path: String,
      sep: String
  ): RDD[((Int, Int), Double)] = {

    val file = sc.textFile(path)
    val ratings = file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) =>
            Some(((cols(0).toInt - 1, cols(1).toInt - 1), cols(2).toDouble))
          case None => None
        }
      })
      .filter({
        case Some(_) => true
        case None    => false
      })
      .map({
        case Some(x) => x
        case None    => ((-1, -1), -1.0)
      })

    return ratings

  }

  def partitionUsers(
      nbUsers: Int,
      nbPartitions: Int,
      replication: Int
  ): Seq[Set[Int]] = {
    val r = new scala.util.Random(1337)
    val bins: Map[Int, collection.mutable.ListBuffer[Int]] =
      (0 to (nbPartitions - 1))
        .map(p => (p -> collection.mutable.ListBuffer[Int]()))
        .toMap
    (0 to (nbUsers - 1)).foreach(u => {
      val assignedBins = r.shuffle(0 to (nbPartitions - 1)).take(replication)
      for (b <- assignedBins) {
        bins(b) += u
      }
    })
    bins.values.toSeq.map(_.toSet)
  }
  def getTimings(
      function: () => (Double, breeze.linalg.CSCMatrix[Double], (Int, Int) => Double),
      num_measurements: Int
  ): (Array[Double], (Double, breeze.linalg.CSCMatrix[Double], (Int, Int) => Double)) = {
    val res = for (i <- 0 until num_measurements) yield { timingInMsCustom(function) }

    return (res.toArray.map(_._2), res(0)._1)
  }


  
  def timingInMsCustom(f: () => (Double, breeze.linalg.CSCMatrix[Double], (Int, Int) => Double)): ((Double, breeze.linalg.CSCMatrix[Double], (Int, Int) => Double), Double) = {
    val start = System.nanoTime()
    val output = f()
    val end = System.nanoTime()
    return (output, (end - start) / 1000000.0)
  }

}
