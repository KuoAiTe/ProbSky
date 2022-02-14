package probsky.method

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_2

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import probsky.method.PSky
import probsky.util.instance.Instance
import probsky.util.point.Point
import probsky.util.function.Util.{computeSkyline, computeSkylineWithCount}

class PSkyNoPivot(sc: SparkContext, srcData: RDD[String], numPartition : Int, dimension: Int, lowerBoundProb: Double, numDominatingCorner: Int, numDominatingInstances: Int, binSize: Int, splitThreshold: Int, randSeed: Long = System.currentTimeMillis())
extends PSky(sc: SparkContext, srcData: RDD[String], numPartition : Int, dimension: Int, lowerBoundProb: Double, numDominatingCorner: Int, numDominatingInstances: Int, binSize: Int, splitThreshold: Int, randSeed: Long) {
  override def Lemma2_Pruning(influentialInstanceRdd: RDD[Instance]) : (Long, RDD[Instance], RDD[(Int, Int)])= {
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
          val (result, dmap) = computeSkylineWithCount(newList, newList)
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
    // --------------------------------------
  }
  override def dominatingInstancesPrune(candidateInstancesRdd: RDD[Instance], dominatingInstancesBroadcasted: Broadcast[Array[Instance]]) = {
    start()
    val dominatingInstances = dominatingInstancesBroadcasted.value
    val dimension = dimensionBroadcasted.value
    val lowerBoundProb = lowerBoundProbBroadcasted.value
    val splitThreshold = splitThresholdBroadcasted.value
    val rand = randBroadcasted.value
    val candidateObjsSet = candidateInstancesRdd.mapPartitionsWithIndex( (partId, partitionIter) => {
      var instList = partitionIter.toArray
      val objProbMap = computeSkyline(instList, dominatingInstances)
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
  override def computeSkyProb(partitionData: RDD[PartitionData]) = {
      val dimension = dimensionBroadcasted.value
      val numPartition = numPartitionBroadcasted.value
      partitionData.mapPartitions {
        partitionIter => {
          partitionIter.flatMap {
            case PartitionData(partId, candInsts, inflInstsOption) => {
              val startTime = System.nanoTime
              val inflInsts = inflInstsOption.getOrElse(Array[Instance]())
              val objProbMap = computeSkyline(candInsts, inflInsts)
              val runningTime = (System.nanoTime - startTime) / 1e9
              println(f" ${runningTime}%.3f s | partition: ${partId} | ${candInsts.size} | ${inflInsts.size}")
              objProbMap
            }
          }
        }
      }.reduceByKey(
       (acc, prob) => (acc + prob)
      )
    }
}
