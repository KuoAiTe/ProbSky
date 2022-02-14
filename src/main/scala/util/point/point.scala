import scala.math
import xxl.core.spatial.points.DoublePoint
import probsky.util.function.Util.DoublePointExtension
import probsky.util.instance.Instance

package probsky.util.point {

    class Point(val dim: Int, val coordinates: Array[Double]) extends Serializable {
      def apply(i: Int) : Double = coordinates(i)
      def update(i: Int, value: Double) = coordinates(i) = value
      def this(dim: Int) = {
        this(dim, new Array[Double](dim))
      }
      def this(pt: Array[Double]) = {
        this(pt.length, pt)
      }
      def setValue(value: Double) = {
        for (i<- 0 until dim)
          coordinates(i) = value
      }
      def setValue(pt: Point) = {
        for (i<- 0 until dim)
          coordinates(i) = pt(i)
      }
      def setValue(arrDouble: Array[Double]) = {
        for (i<- 0 until dim)
          coordinates(i) = arrDouble(i)
      }
      def toArray() = coordinates

      /*
       * if self dominates the given point, returns true
       * if not, return false.
       * Given a set of n dimensional points T
       * A point t1 is said to dominate another point t2
       * if and only if
       * t1 is better than or equal to t2 on all dimensions
       * and t1 is better than t2 on at least one dimension.
       */
      @inline def dominates(point: Point) : Boolean = {
        var result = true
        var betterDimCount = 0
        var i = 0
        while (i < dim) {
          if (coordinates(i) > point.coordinates(i) ) {
            result = false
            i = dim
          } else if (coordinates(i) < point.coordinates(i) ) {
            betterDimCount += 1
          }
          i += 1
        }
        result &= (betterDimCount > 0)
        result
      }
      @inline def dominates(point: DoublePoint) : Boolean = {
        val pt = new DoublePoint(coordinates)
        pt.dominates(point)
      }
      @inline def dominates(inst: Instance) : Boolean = this.dominates(inst.pt)

      @inline def contains(point: Point) : Boolean = {
        var result = true
        var i = 0
        while (i < dim) {
          if (point(i) > coordinates(i)) {
            result = false
            i = dim
          }
          i += 1
        }
        result
      }

      @inline def equals(point: Point) : Boolean = {
        var result = true
        var i = 0
        while (i < dim) {
          if (point(i) - coordinates(i) > 1e10) {
            result = false
            i = dim
          }
          i += 1
        }
        result
      }
      override def toString = coordinates.map("%.2f".format(_)).mkString(",")
    }

}
