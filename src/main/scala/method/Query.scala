package probsky.method

import org.apache.spark.rdd.RDD

abstract class Query () extends Serializable {
  var startTime = 0L
  def start() = {
    startTime = System.nanoTime
  }

  def end(str: String) = {
    val duration = (System.nanoTime - startTime) / 1e9
    println(f" ${str}: ${duration}%.3f s")
  }
  def run() : (Array[(Int, Double)])
}
