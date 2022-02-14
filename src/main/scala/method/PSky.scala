package probsky.method

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_2

import scala.util.Random
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import probsky.method.Query
import probsky.util.instance.Instance
import probsky.util.point.Point
import probsky.util.function.Util.computeSkyline
import probsky.util.function.{ReferencePointAcceleration, ReferencePointForLemma2}


class PSky(sc: SparkContext, srcData: RDD[String], numPartition : Int, dimension: Int, lowerBoundProb: Double, numDominatingCorner: Int, numDominatingInstances: Int, binSize: Int, splitThreshold: Int, randSeed: Long = System.currentTimeMillis())
extends PSkyBase(sc: SparkContext, srcData: RDD[String], numPartition : Int, dimension: Int, lowerBoundProb: Double, numDominatingCorner: Int, binSize: Int, randSeed: Long) {
  val splitThresholdBroadcasted = sc.broadcast(splitThreshold)
  val numDominatingInstancesBroadcasted = sc.broadcast(numDominatingInstances)
  val randBroadcasted = sc.broadcast(rand)

  override def Lemma2_Pruning(influentialInstanceRdd: RDD[Instance]) = {
    start()
    val dimension = dimensionBroadcasted.value
    val lowerBoundProb = lowerBoundProbBroadcasted.value
    val splitThreshold = splitThresholdBroadcasted.value
    val rand = randBroadcasted.value
    val numDominatingInstances = numDominatingInstancesBroadcasted.value
    val binSize = binSizeBroadcasted.value
    val tempRdd = influentialInstanceRdd.mapPartitionsWithIndex(
      (partId, partitionIter) => {
        var instList = partitionIter.toArray
        val numInstances = instList.length
        val numChunk = (numInstances * 1.0f / binSize).ceil.toInt
        val objProbMap = new HashMap[Int, Double]()
        val dMap = new HashMap[Int, Int]()
        var i = 0
        while (i < numChunk) {
          var newList = instList
          if (i < numChunk - 1) {
            newList = instList.take(binSize)
            instList = instList.drop(binSize)
          }
          val algo = new ReferencePointForLemma2(partId, newList, dimension, splitThreshold, rand)
          val (result, dmap) = algo.computeForLemma2(newList)
          for ((objId, prob) <- result) {
            if (!objProbMap.contains(objId))
              objProbMap.update(objId, 0f)
            objProbMap(objId) += prob
          }
          for ((instId, count) <- dmap) {
            dMap(instId) = count
          }
          i += 1
        }
        val dominatingMap = dMap.toSeq.sortWith(_._2 > _._2).take(numDominatingInstances)
        Iterable((objProbMap, dominatingMap)).iterator
      }
    ).persist(MEMORY_ONLY_2)

    val candidateObjsSet = tempRdd.flatMap{
      case (objProbMap, dominatingInstances) => objProbMap
    }.reduceByKey(
     (acc, prob) => (acc + prob)
    ).filter {
     case (objId, prob) => prob >= lowerBoundProb
    }.map {
     case (objId, prob) => objId
    }.collect.toSet

    val candidateObjsBroadcasted = sc.broadcast(candidateObjsSet)
    val candidateObjs = candidateObjsBroadcasted.value

    val candidateInstancesRdd = influentialInstanceRdd.filter(
     instance => candidateObjs.contains(instance.objId)
    ).persist(MEMORY_ONLY_2)

    val dominatingCountRdd = tempRdd.flatMap{
      case (objProbMap, dominatingMap) => dominatingMap
    }.persist(MEMORY_ONLY_2)
    val candCount = candidateInstancesRdd.count
    println(f" Lemma2 Pruning (After): ${candCount} instances")
    println(f" DominatingCountRdd: ${dominatingCountRdd.count} instances")
    candidateObjsBroadcasted.destroy()
    tempRdd.unpersist()
    end("Lemma2 Pruning")
    (candCount, candidateInstancesRdd, dominatingCountRdd)
  }
  override def dominatingInstancesSelection(candCount: Long, influentialInstanceRdd: RDD[Instance], dominatingCountRdd: RDD[(Int, Int)]) = {
    start()
    val dimension = dimensionBroadcasted.value
    val splitThreshold = splitThresholdBroadcasted.value
    val rand = randBroadcasted.value
    val numDominatingInstances = numDominatingInstancesBroadcasted.value
    var dominatingInstancesBroadcasted : Broadcast[Array[Instance]] = null
    val inflCount = influentialInstanceRdd.count
    if (numDominatingInstances > 0) {
      val dominantInstanceIds = dominatingCountRdd.takeOrdered(numDominatingInstances)(Ordering.by(-_._2)).map(_._1).toSet
      val dominatingInstances = influentialInstanceRdd.filter(
        inst => dominantInstanceIds.contains(inst.instId)
      ).collect
      dominatingInstancesBroadcasted = sc.broadcast(dominatingInstances)
    }
    end("Dominating Instances Selection")
    dominatingInstancesBroadcasted

  }
  override def dominatingInstancesPrune(candidateInstancesRdd: RDD[Instance], dominatingInstancesBroadcasted: Broadcast[Array[Instance]]) = {
    start()
    val dominatingInstances = dominatingInstancesBroadcasted.value
    val dimension = dimensionBroadcasted.value
    val lowerBoundProb = lowerBoundProbBroadcasted.value
    val splitThreshold = splitThresholdBroadcasted.value
    val rand = randBroadcasted.value
    val candidateObjsSet = candidateInstancesRdd.mapPartitionsWithIndex( (partId, partitionIter) => {
      val algo = new ReferencePointAcceleration(partId, dominatingInstances, dimension, splitThreshold, rand)
      val objProbMap = algo.compute(partitionIter.toArray)
      objProbMap.iterator
    }).reduceByKey(
     (acc, prob) => (acc + prob)
    ).filter {
     case (objId, prob) => prob >= lowerBoundProb
    }.map {
     case (objId, prob) => objId
    }.collect.toSet

    val candidateObjsBroadcasted = sc.broadcast(candidateObjsSet)
    val candidateObjs = candidateObjsBroadcasted.value

    val newCandidateInstancesRdd = candidateInstancesRdd.filter(
     instance => candidateObjs.contains(instance.objId)
    ).persist(MEMORY_ONLY_2)
    println(f" Dominating Instances Prune (After): ${newCandidateInstancesRdd.count} instances")
    dominatingInstancesBroadcasted.destroy()
    candidateObjsBroadcasted.destroy()
    candidateInstancesRdd.unpersist()
    end("Dominating Instances Prune")
    newCandidateInstancesRdd
  }
  override def gridPartitionOptimization(candidateInstancesRdd: RDD[Instance]) = {
    start()
    val numPartition = numPartitionBroadcasted.value
    val equal = 1.0 / numPartition
    val ratio = new Array[Double](numPartition)
    val boundary = new Array[Long](numPartition)
    val candidateSize = candidateInstancesRdd.count
    var accumulated = 0d
    var i = 0
    while (i < numPartition) {
      ratio(i) = (-1 * accumulated + math.sqrt(accumulated * accumulated + 4 * equal)) / 2
      accumulated += ratio(i)
      i += 1
    }
    i = 0
    val total = ratio.sum
    while (i < numPartition) {
      ratio(i) = ratio(i) / total
      i += 1
    }
    i = 1
    boundary(0) = (candidateSize * ratio(0)).toLong
    while (i < numPartition) {
      ratio(i) = ratio(i - 1) + ratio(i)
      boundary(i) = (candidateSize * ratio(i)).toLong
      i += 1
    }
    val objBoundaryBroadcasted = sc.broadcast(boundary)
    val objBoundary = objBoundaryBroadcasted.value

    candidateInstancesRdd.sortBy(inst => inst.pt(0)).zipWithIndex.mapPartitions(
      partitionIter => {
        var i = 0
        var j = 0
        val bdSize = objBoundary.length
        val instList = partitionIter.toArray
        val partMap = new HashMap[Int, ArrayBuffer[Instance]]
        val instListSize = instList.length
        while (i < bdSize) {
          partMap(i) = new ArrayBuffer[Instance]()
          i += 1
        }
        i = 0
        while (i < instListSize) {
          val (inst, idx) = instList(i)
          var partId = 0
          j = 0
          while (j < bdSize) {
            if (idx < objBoundary(j)) {
              partId = j
              j = bdSize
            }
            j += 1
          }
          partMap(partId) += inst
          i += 1
        }
        partMap.iterator
      }
    ).reduceByKey((x, y) => x++y).map{
      case (partId, arrayBuffer) => (partId, arrayBuffer.toArray)
    }.persist(MEMORY_ONLY_2)
  }
  override def computeSkyProb(partitionData: RDD[PartitionData]) = {
    val dimension = dimensionBroadcasted.value
    val numPartition = numPartitionBroadcasted.value
    val splitThreshold = splitThresholdBroadcasted.value
    val rand = randBroadcasted.value
    partitionData.mapPartitions {
      partitionIter => {
        partitionIter.flatMap {
          case PartitionData(partId, candInsts, inflInstsOption) => {
            val startTime = System.nanoTime
            val inflInsts = inflInstsOption.getOrElse(Array[Instance]())
            val algo = new ReferencePointAcceleration(partId, inflInsts, dimension, splitThreshold, rand)
            val objProbMap = algo.compute(candInsts)
            val runningTime = (System.nanoTime - startTime) / 1e9
            println(f" ${runningTime}%.3f s | partition: ${partId} | ${candInsts.size} | ${inflInsts.size} [ Pivot Points Ratio: ${splitThreshold}]")
            objProbMap
          }
        }
      }
    }.reduceByKey(
     (acc, prob) => (acc + prob)
    )
  }
}
