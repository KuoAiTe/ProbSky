package probsky.method

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_2

import scala.collection.immutable.Set
import scala.collection.immutable.List
import scala.collection.immutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import probsky.util.instance.Instance
import probsky.util.point.Point
import probsky.util.function.Util.computeSkyline



case class PartitionBoundary(partId: Int, max: Point)
case class PartitionData(partId: Int, candidateInstance: Array[Instance], influentialInstance: Option[Array[Instance]])

class PSkyBase(sc: SparkContext, srcData: RDD[String], numPartition : Int, dimension: Int, lowerBoundProb: Double, numDominatingCorner: Int, binSize: Int, randSeed: Long = System.currentTimeMillis()) extends Query {
  val rand : Random = new Random(randSeed)
  val dimensionBroadcasted = sc.broadcast(dimension)
  val lowerBoundProbBroadcasted = sc.broadcast(lowerBoundProb)
  val numPartitionBroadcasted = sc.broadcast(numPartition)
  val numDominatingCornerBroadcasted = sc.broadcast(numDominatingCorner)
  val binSizeBroadcasted = sc.broadcast(binSize)
  def transform(dataIn: RDD[String]) : RDD[Instance] = {
    start()
    val dimension = dimensionBroadcasted.value
    val rdd = dataIn.mapPartitions(
      partitionIter => {
        val totalDimension = dimension + 3
        partitionIter.map(
          line => {
            val tokens = line.split(" ")
            if (tokens.length == totalDimension) {
              val objId = tokens(0).toInt
              val instId = tokens(1).toInt
              val prob = tokens(2).toDouble
              val instance = new Instance(objId, instId, prob, dimension)
              val coordinates = tokens.drop(3) map (_.toDouble)
              instance.setPoint(coordinates)
              instance
            } else {
              throw new Exception("Sth wrong in StringToInstance in Util. (Dimension does not match)")
            }
          }
        )
      }
    ).persist(MEMORY_ONLY_2)
    end(f"#instance: ${rdd.count} | #partition: ${numPartition}")
    rdd
  }
  def run() = {
    val rdd = transform(srcData)
    val objBoundaries = getObjectBoundaries(rdd)
    val influentialInstanceRdd = Lemma1_Pruning(rdd, objBoundaries)
    rdd.unpersist()
    val (candCount, candInsts_1, dominatingCountRdd) = Lemma2_Pruning(influentialInstanceRdd)
    var dominatingInstancesBroadcasted : Broadcast[Array[Instance]] = dominatingInstancesSelection(candCount, influentialInstanceRdd, dominatingCountRdd)
    val candInsts_2 = if (dominatingInstancesBroadcasted != null)
      dominatingInstancesPrune(candInsts_1, dominatingInstancesBroadcasted)
    else
      candInsts_1
    val candInsts = gridPartitionOptimization(candInsts_2)
    val maxPointListBroadcasted = getMaxPoints(candInsts)
    candInsts_2.unpersist()

    val partInflInsts = getPartitionInfluentialInstances(influentialInstanceRdd, maxPointListBroadcasted)
    val partitionData = mergePartitionData(candInsts, partInflInsts)
    val allCandSkyProbRdd = computeSkyProb(partitionData)
    val finalObjSkyProb = getQualifiedObjs(allCandSkyProbRdd)
    influentialInstanceRdd.unpersist()
    dimensionBroadcasted.destroy()
    lowerBoundProbBroadcasted.destroy()
    numPartitionBroadcasted.destroy()
    finalObjSkyProb
  }

  /*
   * transform from RDD[objId, instance)] to RDD[(objId, Item)]
   * 1. transform instance to (objId, instance)
   */
   def getObjectBoundaries(rdd: RDD[Instance]) = {
     start()
     val sample = sc.broadcast(rdd.takeSample(false, 500, randSeed)).value
     val dimension = dimensionBroadcasted.value
     val numDominatingCorner = numDominatingCornerBroadcasted.value
     val boundary = rdd.mapPartitions(partitionIter => {
       val objBoundaries = new HashMap[Int, Array[Double]]()
       val instList = partitionIter.toArray
       val instListSize = instList.length
       var i = 0
       var j = 0
       while (i < instListSize) {
         val inst = instList(i)
         objBoundaries.get(inst.objId) match {
           case Some(pt) => {
             j = 0
             while (j < dimension) {
               pt(j) = pt(j) max inst.pt(j)
               j += 1
             }
           }
           case None => {
             val pt = new Array[Double](dimension)
             j = 0
             while (j < dimension) {
               pt(j) = inst.pt(j)
               j += 1
             }
             objBoundaries(inst.objId) = pt
           }
         }
         i += 1
       }
       objBoundaries.iterator
     }).reduceByKey((x,y) => {
       var j = 0
       while (j < dimension) {
         x(j) = x(j) max y(j)
         j += 1
       }
       x
     }).mapPartitionsWithIndex(
       (partId, partitionIter) => {
         var i = 0
         val size = sample.length
         partitionIter.map{
           case (objId, maxPoint) => {
             val pt = new Point(dimension)
             var dmCount = 0
             pt.setValue(maxPoint)
             i = 0
             while (i < size) {
               if (pt.dominates(sample(i))) {
                 dmCount += 1
               }
               i += 1
             }
             (dmCount, maxPoint)
           }
         }.toSeq.sortWith(_._1 > _._1).take(numDominatingCorner).iterator
     }).takeOrdered(numDominatingCorner)(Ordering.by(-_._1)).map{
       case (dmCount, maxPoint) => {
         val pt = new Point(dimension)
         pt.setValue(maxPoint)
         pt
       }
     }
     end(f"getObjBoundary")
     sc.broadcast(boundary)
   }

  /*
   * Prune 1
   * For every instance in these remained objects, we check if a instance
   * in remained objects is dominated by some objects’s O max .
   * If the dominance relationship occurs, the instance’s skyline
   * probability is marked to 0 for the instance can not become skyline instance.
   */
  def Lemma1_Pruning(rdd: RDD[Instance], objBoundaries: Broadcast[Array[Point]]) = {
    start()
    val boundaryMap = objBoundaries.value
    val boundaryMapSize = boundaryMap.size
    val result = rdd.filter(
      instance => {
        var i = 0
        var result = true
        while (i < boundaryMapSize) {
          if (boundaryMap(i).dominates(instance.pt)) {
            result = false
            i = boundaryMapSize
          }
          i = i + 1
        }
        result
      }
    ).persist(MEMORY_ONLY_2)
    println(f" Lemma1 Pruning (After): ${result.count} instances")
    end("Lemma1 Pruning")
    result
  }

  def Lemma2_Pruning(influentialInstanceRdd: RDD[Instance]) : (Long, RDD[Instance], RDD[(Int, Int)])= {
    start()
    val dimension = dimensionBroadcasted.value
    val lowerBoundProb = lowerBoundProbBroadcasted.value
    val binSize = binSizeBroadcasted.value
    val candidateObjsSet = influentialInstanceRdd.mapPartitions( partitionIter => {
      var instList = partitionIter.toArray
      val numInstances = instList.length
      val numChunk = (numInstances * 1.0f / binSize).ceil.toInt
      val objProbMap = new HashMap[Int, Double]()
      var i = 0
      while (i < numChunk) {
        var newList = instList
        if (i < numChunk - 1) {
          newList = instList.take(binSize)
          instList = instList.drop(binSize)
        }
        val result = computeSkyline(newList, newList)
        for ((objId, prob) <- result) {
          if (!objProbMap.contains(objId))
            objProbMap.update(objId, 0f)
          objProbMap(objId) += prob
        }
        i += 1
      }
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

    val candidateInstancesRdd = influentialInstanceRdd.filter(
      instance => candidateObjs.contains(instance.objId)
    ).persist(MEMORY_ONLY_2)
    val candCount = candidateInstancesRdd.count
    println(f" Lemma2 Pruning (After): ${candCount} instances")
    end("Lemma2 Pruning")
    (candCount, candidateInstancesRdd, null)
    // --------------------------------------
  }
  def dominatingInstancesSelection(candCount: Long, influentialInstanceRdd: RDD[Instance], dominatingCountRdd: RDD[(Int, Int)]) : Broadcast[Array[Instance]] = {
    null
  }
  def dominatingInstancesPrune(candidateInstancesRdd: RDD[Instance], dominatingInstancesBroadcasted: Broadcast[Array[Instance]]) = {
    candidateInstancesRdd
  }

  def gridPartitionOptimization(candidateInstancesRdd: RDD[Instance]) = {
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

  def getPartitionInfluentialInstances(influentialInstanceRdd: RDD[Instance], maxPointListBroadcasted: Broadcast[Array[Point]]) = {
    start()
    val maxPointList = maxPointListBroadcasted.value
    influentialInstanceRdd.mapPartitionsWithIndex(
      (partId, partitionIter) => {
        val partMap = new HashMap[Int, ArrayBuffer[Instance]]
        val instList = partitionIter.toArray
        var i = 0
        var j = 0
        val maxPointListSize = maxPointList.length
        val instListSize = instList.length
        while (i < maxPointListSize) {
          val maxPoint = maxPointList(i)
          partMap(i) = new ArrayBuffer[Instance]()
          j = 0
          while (j < instListSize) {
            if (instList(j).pt.dominates(maxPoint)) {
              partMap(i) += instList(j)
            }
            j += 1
          }
          i += 1
        }
        partMap.iterator
      }
    ).reduceByKey((x,y) => x++y).map{
      case (partId, arrayBuffer) => (partId, arrayBuffer.toArray)
    }
  }

  /*
   * maxPointList generation: [partID, MAXPoint]
   * a. gather all candidate obj lists in a partition, looks for max coner point
   * b. collect (partID, maxPoint) as a list
   * Make the transformation RDD(elem)->Array(elem)
   */
  def getMaxPoints(candidateInstances: RDD[(Int, Array[Instance])]) = {
    start()
    val dimension = dimensionBroadcasted.value
    val result = candidateInstances.mapPartitionsWithIndex {
      (partId, partitionIter) => {
        partitionIter.map {
          case (partId, pointSet) => {
            val maxPoint = pointSet.map(_.pt).fold(new Point(dimension))(
              (maxPt, pt) => {
                var i = 0
                while (i < dimension) {
                  if (pt(i) > maxPt(i)) {
                    maxPt(i) = pt(i)
                  }
                  i += 1
                }
                maxPt
              }
            )
            PartitionBoundary(partId, maxPoint)
          }
        }
      }
    }.collect.sortBy{
      case PartitionBoundary(partId, maxPoint) => partId
    }.map{
      case PartitionBoundary(partId, maxPoint) => maxPoint
    }
    end("getMaxPoints")
    sc.broadcast(result)
  }

  def mergePartitionData(partitionCandidateInstances: RDD[(Int, Array[Instance])], partitionInfluentialInstance: RDD[(Int, Array[Instance])]) = {
    partitionCandidateInstances.leftOuterJoin(partitionInfluentialInstance).map( tuple => PartitionData(tuple._1, tuple._2._1, tuple._2._2))
  }

  def computeSkyProb(partitionData: RDD[PartitionData]) = {
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
  def getQualifiedObjs(allCandSkyProb: RDD[(Int, Double)]) = {
    start()
    val lowerBound = lowerBoundProbBroadcasted.value
    val newParititonSize = math.max(numPartition / 4, 1)
    val result = allCandSkyProb.filter {
      case (objId, objProb) => objProb >= lowerBound
    }.coalesce(newParititonSize).collect
    end("getQualifiedObjs")
    result
  }
}
