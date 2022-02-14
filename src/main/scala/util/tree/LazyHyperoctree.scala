
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import probsky.util.instance.Instance

package probsky.util.tree {
  class LazyHyperoctree(val samplePoints: Array[Instance], val dimension: Int, val splitThreshold: Int) extends Serializable {
    def getBounds(candInsts: Array[Instance], reduceFunction: (Double, Double) => Double) = {
      val initPoint = new Array[Double](dimension)
      candInsts.foldLeft(initPoint)(
        (leftPt, pt) => {
          var i = 0
          while (i < dimension) {
            leftPt(i) = reduceFunction(leftPt(i), pt.pt(i))
            i += 1
          }
          leftPt
        }
      )
    }

    def genQtree() = {
      val start = System.nanoTime
      val low = getBounds(samplePoints, math.min)
      val high = getBounds(samplePoints, math.max)
      val samplePointList = samplePoints.to(ArrayBuffer)
      val root = new LazyHyperoctreeNode(0, dimension, low, high)
      val (splitNodes, instaceNodeMap, length) = root.split(0, samplePointList, splitThreshold)
      val runningTime = (System.nanoTime - start) / 1e9
      (root, splitNodes, instaceNodeMap)
    }

  }


  class LazyHyperoctreeNode(val nodeId: Int, val dimension: Int, val min: Array[Double], val max: Array[Double]) extends Serializable {
    var instIdx : Array[Int] = null
    override def toString() = f"min: ${min.map("%.2f".format(_)).mkString(",")} | max:${max.map("%.2f".format(_)).mkString(",")} \n"

    @inline def findNodeIndex(instance: Instance) : Int = {
      var nodeId = 0
      var i = 0
      while (i < dimension) {
        val midPoint = (min(i) + max(i)) / 2
        if (instance.pt(i) >= midPoint) {
          nodeId |= (1 << (dimension - i - 1))
        }
        i += 1
      }
      nodeId
    }

    def split(idHolder: Int, instanceList: ArrayBuffer[Instance], splitThreshold: Int) : (ArrayBuffer[LazyHyperoctreeNode], HashMap[Int,  Int], Int) = {
      var instSize = instanceList.length
      var thresholdMap = new HashMap[Int, Int]()
      var nodeInstanceMap = new HashMap[Int, ArrayBuffer[Instance]]()
      var instaceNodeMap = new HashMap[Int,  Int]()
      val splitNodes = new ArrayBuffer[LazyHyperoctreeNode]()
      var lastId = idHolder
      splitNodes += this
      lastId += 1
      val start = System.nanoTime
      var i = 0
      var j = 0

      while (i < instSize) {
        val inst = instanceList(i)
        instaceNodeMap(inst.instId) = this.nodeId
        i += 1
      }
      i = 0
      var nodeId = 0
      while (i < instSize) {
        nodeId = findNodeIndex(instanceList(i))
        if (!thresholdMap.contains(nodeId)) {
          thresholdMap(nodeId) = 0
        }
        thresholdMap(nodeId) += 1
        i += 1
      }
      val qualifiedChildren = thresholdMap.filter(_._2 > splitThreshold).toArray
      val qualifiedNodeIds = qualifiedChildren.map(_._1).toSet
      for (nodeId <- qualifiedNodeIds) {
        nodeInstanceMap(nodeId) = new ArrayBuffer[Instance]()
      }
      i = 0
      while (i < instSize) {
        val instance = instanceList(i)
        nodeId = findNodeIndex(instance)
        if (qualifiedNodeIds.contains(nodeId)) {
          nodeInstanceMap(nodeId) += instance
        }
        i += 1
      }
      // never carries any points
      instanceList.clear()

      // recursively split the subspaces
      i = 0
      var k = 0
      while (i < qualifiedChildren.length) {
        val (nodeId, instCount) = qualifiedChildren(i)
        val instList = nodeInstanceMap(nodeId)
        val childMin = instList(0).pt.coordinates.clone
        val childMax = childMin.clone
        j = 1
        while (j < instCount) {
          k = 0
          while (k < dimension) {
            childMin(k) = math.min(childMin(k), instList(j).pt.coordinates(k))
            childMax(k) = math.max(childMax(k), instList(j).pt.coordinates(k))
            k += 1
          }
          j += 1
        }
        val node = new LazyHyperoctreeNode(lastId, dimension, childMin, childMax)
        if (!(childMin sameElements childMax)) {
          val (nodes, inMap, tempId) = node.split(lastId, nodeInstanceMap(nodeId), splitThreshold)
          for ((instId, nodeId) <- inMap) {
            instaceNodeMap(instId) = nodeId
          }
          splitNodes ++= nodes
          lastId = tempId
        }
        i += 1
      }
      (splitNodes, instaceNodeMap, lastId)
    }

  }

}
