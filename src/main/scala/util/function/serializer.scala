package probsky.util.function.serializer

import org.apache.spark.serializer.KryoRegistrator
import com.twitter.chill.Kryo
import probsky.util.instance._
import probsky.util.point._
import probsky.util.tree._
import probsky.method._

class ProbSkyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) = {
    kryo.register(classOf[java.util.Random])
    kryo.register(classOf[scala.util.Random])
    kryo.register(classOf[Instance])
    kryo.register(classOf[CandidateInstance])
    kryo.register(classOf[Point])
    kryo.register(classOf[PSky])
    kryo.register(classOf[PSkyBase])
    kryo.register(classOf[Array[Instance]])
    kryo.register(classOf[Array[CandidateInstance]])
    kryo.register(classOf[Array[Point]])
    kryo.register(classOf[PSQPFMRInstance])
    kryo.register(classOf[Array[PSQPFMRInstance]])
    kryo.register(classOf[PSQTree])
    kryo.register(classOf[PSQNode])
    kryo.register(classOf[LazyHyperoctree])
    kryo.register(classOf[LazyHyperoctreeNode])
    kryo.register(classOf[Array[PSQNode]])
    kryo.register(classOf[scala.math.Ordering[_]])
    kryo.register(classOf[java.util.concurrent.atomic.AtomicLong])
    kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[java.lang.invoke.SerializedLambda])
    kryo.register(classOf[PartitionBoundary])
    kryo.register(classOf[Array[PartitionBoundary]])
    kryo.register(classOf[PartitionData])
    kryo.register(Class.forName("scala.math.Ordering$$anon$1"))
    kryo.register(Class.forName("scala.math.Ordering$$anon$6"))
    kryo.register(Class.forName("scala.reflect.ClassTag$GenericClassTag"))
  }
}
