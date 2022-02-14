package probsky.util.function
import scala.util.Random
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import probsky.util.instance.Instance
import probsky.util.point.Point
import probsky.util.tree.{LazyHyperoctree, LazyHyperoctreeNode}


class ReferencePointAcceleration(partId: Int, instList: Array[Instance], dimension: Int, splitThreshold: Int, rand : Random) extends Serializable {
  var nodes = new Array[LazyHyperoctreeNode](0)
  var instaceNodeMap = new HashMap[Int,  Int]()
  var instancesInsideNode = new Array[Array[Int]](0)
  val instIdx: Array[Int] = Array.range(0, instList.length)
  var refPointsSize : Int = 0
  def getCandidateObjs(candInsts: Array[Instance]) = {
    var i = 0
    var j = 0
    val objMap = new HashMap[Int, Double]()
    val candInstSize = candInsts.length
    var actual : Long = refPointsSize * instList.length
    val expected : Long = candInstSize.toLong * instList.length
    val startTime = System.nanoTime
    while (i < candInstSize) {
      val inst = candInsts(i)
      val testInstIdxs = instancesInsideNode(instaceNodeMap(inst.instId))
      val testInstSize = testInstIdxs.length
      val aggregatedProb = new HashMap[Int, Double]()
      j = 0
      while (j < testInstSize) {
        val testInst = instList(testInstIdxs(j))
        if (testInst.pt.dominates(inst.pt)) {
          if (!aggregatedProb.contains(testInst.objId)) {
            aggregatedProb.update(testInst.objId, 0.0)
          }
          aggregatedProb(testInst.objId) += testInst.prob
        }
        j += 1
      }
      if (!objMap.contains(inst.objId)) {
        objMap(inst.objId) = 0
      }
      aggregatedProb(inst.objId) = 0
      objMap(inst.objId) += inst.prob * aggregatedProb.foldLeft(1.0){
        case (accumulator, (objId, prob)) => accumulator * (1 - prob)
      }
      actual += testInstSize
      i += 1
    }
    val runningTime = (System.nanoTime - startTime) / 1e9
    val percent = if (expected != 0) 100 * (expected * 1.0 - actual ) / expected else 0
    //println(f" Pivot Point Compute - [${partId}]: ${runningTime}%.3f s | ${candInstSize} |  ${candInstSize} [${percent}%.2f%%] -> ${actual} / ${expected}")
    objMap
  }

  def buildTree(candInsts: Array[Instance]) = {
    val startTime = System.nanoTime
    val (rootNode, refPoints, map) = new LazyHyperoctree(candInsts, dimension, splitThreshold).genQtree()
    nodes = refPoints.toArray
    instaceNodeMap = map
    rootNode.instIdx = instIdx
    refPointsSize = refPoints.length
    instancesInsideNode = new Array[Array[Int]](refPointsSize)
    instancesInsideNode(0) = Array.range(0, instList.length)
    var i = 0
    var j = 0
    val instListSize = instList.length
    while (i < refPointsSize) {
      val refPoint = refPoints(i)
      val maxPt = new Point(refPoint.max)
      val buffer = new ArrayBuffer[Int]()
      j = 0
      while (j < instListSize) {
        if (maxPt.contains(instList(j).pt)) {
          buffer += j
        }
        j += 1
      }
      instancesInsideNode(i) = buffer.toArray
      i += 1
    }
    //println(f" Pivot Point Build - [${partId}]: ${runningTime}%.3f s")
  }
  def compute(candInsts: Array[Instance]) = {
    buildTree(candInsts)
    getCandidateObjs(candInsts)
  }
}
