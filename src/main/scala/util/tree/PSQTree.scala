package probsky.util.tree
import scala.math
import scala.collection.mutable.ArrayBuffer
import probsky.util.instance.PSQPFMRInstance
import probsky.util.point.Point

/* Generating a PS-QTREE
 * We recursively divided-dimensional space into equi-sized 2^d sub-spaces,
 * each of which is associated with a node in a PS-QTREE,
 * until the number of points in each sub-space does not exceed thesplit threshold, denoted by ρ.
 * We refer to the regionrepresented by a node n as
 * n.region=〈[n(1)−,n(1)+),···,[n(d)−,n(d)+)〉where[n(k)−,n(k)+)is the k-th dimensional range.
 * We also define n.min(n.max) as then.region’s closest (farthest) corner of a leaf nodenfrom the origin.
 * Each node n is assigned with an id according to the method in [40] and the node with an id “i” isrepresented bynode(i).
 * To build a PS-QTREE quickly, we utilize a random sample S of the objects in D.
 * Figure 6.1 shows an example of a PS-QTREE produced by the subset S = {W,Z} of D in Figure 3.4(a)
  */
class PSQTree(val sampleObjs: Array[(Int, Array[PSQPFMRInstance])], val dimension: Int, val splitThreshold: Int, val valueBound: Point) extends Serializable {
  val childrenSize = 1 << dimension
  var rootNode: PSQNode = null
  var leafNodes : Array[PSQNode] = null
  def genQtree() : PSQTree = {
    rootNode = split(splitThreshold)
    leafNodes = getLeafNodesFromRootNode(rootNode).toArray
    computeLeafNodesProbabilityMin()
    this
  }
  def split(splitThreshold: Int) : PSQNode = {
    val start = System.nanoTime
    val low = new Point(dimension)
    val high = new Point(dimension)
    high.setValue(valueBound)
    val samplePointList = sampleObjs.flatMap(_._2).to(ArrayBuffer)
    val root = new PSQNode(0, "", samplePointList, dimension, low, high)
    root.split(splitThreshold)
    //println("split: " + (System.nanoTime - start) / 1e9 + "s")
    root
  }
  def findNode(instance: PSQPFMRInstance): PSQNode = {
    var correspondingNode : PSQNode = null
    var i = 0
    var j = 0
    val leafNodeSize = leafNodes.length
    while (i < leafNodeSize) {
      var leafNode = leafNodes(i)
      var instanceInLeafNodeRegion = true
      j = 0
      while ( j < dimension) {
        if (instance.pt(j) < leafNode.min(j) || instance.pt(j) >= leafNode.max(j)) {
          instanceInLeafNodeRegion = false
          j = dimension
        }
        j += 1
      }
      if (instanceInLeafNodeRegion) {
        correspondingNode = leafNode
        instance.leafNode = correspondingNode
        i = leafNodes.size
      }
      i += 1
    }
    correspondingNode
  }

  def findNode(pt: Point): PSQNode = {
    var correspondingNode : PSQNode = null
    val leafNodeSize = leafNodes.length
    var i = 0
    var j = 0
    while (i < leafNodeSize) {
      val leafNode = leafNodes(i)
      var instanceInLeafNodeRegion = true
      j = 0
      while (j < dimension) {
        if (pt(j) < leafNode.min(j) || pt(j) >= leafNode.max(j)) {
          instanceInLeafNodeRegion = false
          j = dimension
        }
        j += 1
      }
      if (instanceInLeafNodeRegion) {
        correspondingNode = leafNode
      }
      i += 1
    }
    correspondingNode
  }
  /*

    def findNode(instance: PSQPFMRInstance, node: PSQNode): PSQNode = {
      var correspondingNode : PSQNode = null
      val child = node.findNode(instance)
      if (!child.leafNode) {
        correspondingNode = findNode(instance, child)
      } else {
        correspondingNode = node
      }
      correspondingNode
    }
    */
  /*
   * See DEFINITION 4.12
   * 1. Scan every object V belonging to S
   * 2. Foreach V, Compute A = the sum of P(v_{j}) of all instances v_{j} dominating n.min.
   * 3. Foreach V, Compute (1 - A)
   * 4. Product all the numbers from 3 and obtain n.P_{min}(S)
   */
  def computeLeafNodesProbabilityMin() = {
    val start = System.nanoTime
    for (leafNode <- leafNodes) {
      var i = 0
      leafNode.probabilityMin = sampleObjs.map{
        case (objId, instanceList) => {
          i = 0
          var result = 1d
          while (i < instanceList.length) {
            val instance = instanceList(i)
            if (instance.pt.dominates(leafNode.min)) {
              result -= instance.prob
            }
            i += 1
          }
          result
        }
      }.product
      //println("leafNode: " + leafNode.id + "(" + leafNode.min + ")"+ ": " + leafNode.probabilityMin )
      // clean all instances belonging to the leaf node
      // there's no need to keep those instances as we only need pMin for further computation.
      // It reduces the memory usage and thus more quick to broadcast
      leafNode.instanceList.clear()
      //println(leafNode.id + " size: " + leafNode.instanceList.length)
    }
    println("leafNode size: " + leafNodes.length)
    //println("computeLeafNodesProbabilityMin: " + (System.nanoTime - start) / 1e9 + "s")
  }

  def getLeafNodesFromRootNode(node: PSQNode) : ArrayBuffer[PSQNode] = {
    val start = System.nanoTime
    val leafNodes = new ArrayBuffer[PSQNode]()
    var i = 0
    while (i < childrenSize) {
      if (node.child(i) != null) {
        val visitingNode = node.child(i)
        if (visitingNode.leafNode)
          leafNodes += visitingNode
        else
          leafNodes ++= getLeafNodesFromRootNode(visitingNode)
      }
      i += 1
    }
    //println("getLeafNodesFromRootNode: " + (System.nanoTime - start) / 1e9 + "s")
    leafNodes
  }

}

class PSQNode(val depth : Int, val id: String, val instanceList: ArrayBuffer[PSQPFMRInstance], val dimension: Int, val min: Point, val max: Point) extends Serializable {
  val childrenSize = 1 << dimension
  // internal nodes have exactly 2^d child
  val child = new Array[PSQNode](childrenSize)
  var leafNode = false
  var probabilityMin : Double = 1f
  override def toString() = {
    s"id: ${id}\n min: ${min}\n max:${max} \n"
  }
  /*
   * For a pair of nodes n_{1} and n_{2} in a PSQtree,
   * if n_{1}.min(k) < n_{2}.max(k) for k = 1, ..., d
   * we say n_{1} weakly dominates n_{2} and represent it by n_{1} \prec n_{2}
   */
  def dominates(node: PSQNode) = {
    var result = true
    var i = 0
    while (result && i < dimension) {
      if (min(i) >= node.max(i) ) {
        result = false
      }
      i += 1
    }
    result
  }

  def getIndex(instance: PSQPFMRInstance) : Int = {
    var index = -1
    if (child != null) {
      index = 0
      for ( i <- 0 until dimension) {
        val midPoint = (min(i) + max(i)) / 2
        if (instance.pt(i) >= midPoint) {
          index |= (1 << (dimension - i - 1))
        }
      }
    }
    index
  }

  def findNode(instance: PSQPFMRInstance) : PSQNode = {
    val nodeIndex = getIndex(instance)
    if (nodeIndex != -1)
      child(nodeIndex)
    else
      null
  }
  def split(splitThreshold: Int) : Unit = {
    val start = System.nanoTime
    val childMin = new Array[Point](childrenSize)
    val childMax = new Array[Point](childrenSize)
    for (i <- 0 until childrenSize) {
        val childInstanceList = new ArrayBuffer[PSQPFMRInstance]()
        childMin(i) = new Point(dimension)
        childMax(i) = new Point(dimension)
        val remainingIndex = Array.ofDim[Char](dimension)
        for (j <- 0 until dimension) {
          val midPoint = (min(j) + max(j)) / 2
          if ((i & (1<<j)) == 0) {
            childMin(i)(j) = min(j)
            childMax(i)(j) = midPoint
            remainingIndex(j) = '0'
          } else {
            childMin(i)(j) = midPoint
            childMax(i)(j) = max(j)
            remainingIndex(j) = '1'
          }
        }
        val childId = id + remainingIndex.mkString("")
        var index = Integer.parseInt(remainingIndex.mkString(""), 2)
        child(index) = new PSQNode(depth + 1, childId, childInstanceList, dimension, childMin(i), childMax(i))
    }
    // move point to the corresponding child
    for {
      instance <- instanceList
      node = findNode(instance)
      if node != null
    } { node.instanceList += instance }
    val parentSize = instanceList.length
    // internal nodes don't carry any points
    instanceList.clear

    // recursively split the subspaces
    for {
      i <- 0 until childrenSize
      if child(i) != null
    } {
      // if parentSize == child(i).instanceList.length, it's most likely going to be an infinite loop
      // if there're so many identical features in the data, PSQTree fails to seperate them
      if (child(i).instanceList.length > splitThreshold && parentSize != child(i).instanceList.length) {
        child(i).split(splitThreshold)
      } else {
        child(i).leafNode = true
        //println(child(i).id + " -" + child(i).instanceList.length + " time: " + (System.nanoTime - start) / 1e9 + "s")
      }
    }


  }
}
