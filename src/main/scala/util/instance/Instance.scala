import scala.math
import xxl.core.spatial.points.DoublePoint
import probsky.util.point.Point
import probsky.util.tree.{LazyHyperoctreeNode, PSQNode}

package probsky.util.instance {

  class Instance(val objId: Int, val instId: Int, val prob: Double, val dim: Int) extends Serializable{
    val pt: Point = new Point(dim)
    @inline def dominates(inst: Instance) = pt.dominates(inst.pt)
    @inline def dominates(pt: Point) = pt.dominates(pt)
    def this(inst: Instance) = {
      this(inst.objId, inst.instId, inst.prob, inst.dim)
      pt.setValue(inst.pt)
    }
    def setPoint(arr: Array[Double]) = {
      pt.setValue(arr)
    }
    def setPoint(p: Point) = {
      pt.setValue(p)
    }
    def toCandidateInstance() = {
      val k = new CandidateInstance(objId, instId, prob, dim)
      k.setPoint(pt)
      k
    }
    override def toString() = f"${objId}-${instId}-${prob}: ${pt}\n"
  }

  class CandidateInstance(objId: Int, instId: Int, prob: Double, dim: Int) extends Instance(objId: Int, instId: Int, prob: Double, dim: Int) {
    var node: LazyHyperoctreeNode = null
  }
  class PSQPFMRInstance(override val objId: Int, override val instId: Int, override  val prob: Double, override val dim: Int) extends Instance(objId, instId, prob, dim){
  	var leafNode : PSQNode = null
  	var dominatingPower : Double = 0.0
  }


}
