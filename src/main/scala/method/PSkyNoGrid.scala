package probsky.method

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_2
import scala.util.Random
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import probsky.method.PSky
import probsky.util.instance.Instance
import probsky.util.point.Point
import probsky.util.function.Util

class PSkyNoGrid(sc: SparkContext, srcData: RDD[String], numPartition : Int, dimension: Int, lowerBoundProb: Double, numDominatingCorner: Int, numDominatingInstances: Int, binSize: Int, splitThreshold: Int, randSeed: Long = System.currentTimeMillis())
extends PSky(sc: SparkContext, srcData: RDD[String], numPartition : Int, dimension: Int, lowerBoundProb: Double, numDominatingCorner: Int, numDominatingInstances: Int, binSize: Int, splitThreshold: Int, randSeed: Long) {
  override def gridPartitionOptimization(candidateInstancesRdd: RDD[Instance]) = {
    val candidateSize = candidateInstancesRdd.count
    val numPartition = numPartitionBroadcasted.value
    val ptSize = 1.0 * candidateSize / numPartition
    candidateInstancesRdd.zipWithIndex.mapPartitionsWithIndex(
      (partId, partitionIter) => {
        var i = 0
        val instList = partitionIter.toArray
        val partMap = new HashMap[Int, ArrayBuffer[Instance]]
        val instListSize = instList.length
        while (i < numPartition) {
          partMap(i) = new ArrayBuffer[Instance]()
          i += 1
        }
        i = 0
        while (i < instListSize) {
          val (inst, id) = instList(i)
          val partId = (id / ptSize).toInt
          partMap(partId) += inst
          i += 1
        }
        partMap.iterator
      }
    ).reduceByKey((x, y) => x++y).map{
      case (partId, arrayBuffer) => (partId, arrayBuffer.toArray)
    }.persist(MEMORY_ONLY_2)
  }
}
