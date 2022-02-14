package probsky.util.function

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import probsky.util.instance.Instance
import probsky.util.point.Point
import probsky.util.tree.LazyHyperoctree

class ReferencePointForLemma2(partId: Int, instList: Array[Instance], dimension: Int, splitThreshold: Int, rand : Random) extends ReferencePointAcceleration(partId: Int, instList: Array[Instance], dimension: Int, splitThreshold: Int, rand : Random) {
    def getCandidateObjsForLemma2(candInsts: Array[Instance]) = {
      var i = 0
      var j = 0
      val objMap = new HashMap[Int, Double]()
      val dMap = new HashMap[Int, Int]()
      val candInstSize = candInsts.length
      var actual : Long = refPointsSize.toLong * instList.length
      val expected : Long = candInstSize.toLong * instList.length
      val startTime = System.nanoTime
      var inst : Instance = null
      var testInstIdxs: Array[Int] = null
      var testInstSize: Int = 0
      var testInst: Instance = null
      var testInstId: Int = 0
      var testInstObjId: Int = 0
      while (i < candInstSize) {
        inst = candInsts(i)
        testInstIdxs = instancesInsideNode(instaceNodeMap(inst.instId))
        testInstSize = testInstIdxs.length
        val aggregatedProb = new HashMap[Int, Double]()
        j = 0
        while (j < testInstSize) {
          testInst = instList(testInstIdxs(j))
          if (testInst.pt.dominates(inst.pt)) {
            testInstObjId = testInst.objId
            testInstId = testInst.instId
            if (!aggregatedProb.contains(testInstObjId)) {
              aggregatedProb.update(testInstObjId, 0.0)
            }
            if (!dMap.contains(testInstId)) {
              dMap.update(testInstId, 0)
            }
            aggregatedProb(testInstObjId) += testInst.prob
            dMap(testInstId) += 1
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
      (objMap, dMap)
    }

    def computeForLemma2(candInsts: Array[Instance]) = {
      buildTree(candInsts)
      getCandidateObjsForLemma2(candInsts)
    }

    /*
     * it finds the pivot point combination with the largest optimization power.
     * get the best pivot point and add points to that rectangles
     */
    override def buildTree(candInsts: Array[Instance]) = {
       val startTime = System.nanoTime
       val (rootNode, refPoints, map) = new LazyHyperoctree(candInsts, dimension, splitThreshold).genQtree()
       nodes = refPoints.toArray
       instaceNodeMap = map
       refPointsSize = refPoints.length
       instancesInsideNode = new Array[Array[Int]](refPointsSize)
       instancesInsideNode(0) = Array.range(0, instList.length)
       var i = 1
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
     }
}
