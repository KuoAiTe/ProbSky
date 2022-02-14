
import scala.collection.mutable.ListBuffer
import java.io.File
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.io.Source
import scala.math
import xxl.core.spatial.points.DoublePoint
import probsky.util.instance.{Instance, PSQPFMRInstance}

package probsky.util.function {
  object Util {
    implicit class DoublePointExtension(pt: DoublePoint) {
      def checkDomination(pt1: Array[Double], pt2: Array[Double]) = {
       var result = true
       var betterDimCount = 0
       var i = 0
       val dim = pt1.length
       while (result && i < dim) {
         if (pt1(i) > pt2(i) ) {
           result = false
         } else if (pt1(i) < pt2(i) ) {
           betterDimCount += 1
         }
         i += 1
       }
       result &= (betterDimCount > 0)
       result
      }

      def dominates( pt2: DoublePoint): Boolean = checkDomination(pt.getPoint().asInstanceOf[Array[Double]], pt2.getPoint().asInstanceOf[Array[Double]])

    }

    import org.apache.spark.rdd.RDD
    def computeSkyline(candInsts: Array[Instance], inflInsts: Array[Instance]) = {
      val objProbMap = new HashMap[Int, Double]()
      var i = 0
      var j = 0
      val candInstSize = candInsts.size
      val inflInstSize = inflInsts.size
      while (i < candInstSize) {
        val inst = candInsts(i)
        val aggregatedProb = new HashMap[Int, Double]()
        j = 0
        while (j < inflInstSize) {
          val testInst = inflInsts(j)
          if (testInst.pt.dominates(inst.pt)) {
            if (!aggregatedProb.contains(testInst.objId)) {
              aggregatedProb.update(testInst.objId, 0.0)
            }
            aggregatedProb(testInst.objId) += testInst.prob
          }
          j += 1
        }
        if (!objProbMap.contains(inst.objId)) {
          objProbMap(inst.objId) = 0
        }
        aggregatedProb(inst.objId) = 0
        objProbMap(inst.objId) += inst.prob * aggregatedProb.foldLeft(1.0){
          case (accumulator, (objId, prob)) => accumulator * (1 - prob)
        }
        i += 1
      }
      objProbMap
    }

    def computeSkylineWithCount(candInsts: Array[Instance], inflInsts: Array[Instance]) = {
      val objProbMap = new HashMap[Int, Double]()
      val dominatingCountMap = new HashMap[Int, Int]()
      var i = 0
      var j = 0
      val candInstSize = candInsts.size
      val inflInstSize = inflInsts.size
      while (i < candInstSize) {
        val inst = candInsts(i)
        val aggregatedProb = new HashMap[Int, Double]()
        j = 0
        while (j < inflInstSize) {
          val testInst = inflInsts(j)
          if (testInst.pt.dominates(inst.pt)) {
            if (!aggregatedProb.contains(testInst.objId)) {
              aggregatedProb.update(testInst.objId, 0.0)
            }
            if (!dominatingCountMap.contains(testInst.instId)) {
              dominatingCountMap.update(testInst.instId, 0)
            }
            aggregatedProb(testInst.objId) += testInst.prob
            dominatingCountMap(testInst.instId) += 1
          }
          j += 1
        }
        if (!objProbMap.contains(inst.objId)) {
          objProbMap(inst.objId) = 0
        }
        aggregatedProb(inst.objId) = 0
        objProbMap(inst.objId) += inst.prob * aggregatedProb.foldLeft(1.0){
          case (accumulator, (objId, prob)) => accumulator * (1 - prob)
        }
        i += 1
      }
      (objProbMap, dominatingCountMap)
    }

    def computeDominatingCount(candInsts: Array[Instance], inflInsts: Array[Instance]) = {
      val dominatingCountMap = new HashMap[Int, Int]()
      var i = 0
      var j = 0
      val candInstSize = candInsts.size
      val inflInstSize = inflInsts.size
      while (i < candInstSize) {
        val inst = candInsts(i)
        j = 0
        while (j < inflInstSize) {
          val testInst = inflInsts(j)
          if (testInst.pt.dominates(inst.pt)) {
            if (!dominatingCountMap.contains(testInst.instId)) {
              dominatingCountMap.update(testInst.instId, 0)
            }
            dominatingCountMap(testInst.instId) += 1
          }
          j += 1
        }
        i += 1
      }
      dominatingCountMap
    }
    /*
     * given a string, stringToInstance creates an instance.
     * objectID, instanceId, probability, dim_{1}, dim_{2}, dim_{3}, ..., dim_{n}
     */
    @inline def stringToInstance(line: String, dim: Int) : (Int, Instance) = {
      val tokens = line.split(" ");
      if (tokens.length == dim + 3) {
        val objId = tokens(0).toInt
        val instId = tokens(1).toInt
        val prob = tokens(2).toDouble
        val instance = new Instance(objId, instId, prob, dim);
        val coordinates = tokens.drop(3) map (_.toDouble)
        instance.setPoint(coordinates)
        (objId, instance)
      } else {
        throw new Exception("Sth wrong in StringToInstance in Util. (Dimension does not match)")
      }
    }


    /*
     * given a string, stringToInstance creates an instance.
     * objectID, instanceId, probability, dim_{1}, dim_{2}, dim_{3}, ..., dim_{n}
     */
    def stringToPSQPFMRInstance(line: String, dim: Int) : (Int, PSQPFMRInstance) = {
      val tokens = line.split(" ");
      if (tokens.length == dim + 3) {
        val objId = tokens(0).toInt
        val instId = tokens(1).toInt
        val prob = tokens(2).toDouble
        val instance = new PSQPFMRInstance(objId, instId, prob, dim);
        val coordinates = tokens.drop(3) map (_.toDouble)
        instance.setPoint(coordinates)
        (objId, instance)
      } else {
        throw new Exception("Sth wrong in StringToInstance in Util. (Dimension does not match)")
      }
    }


  }

}
