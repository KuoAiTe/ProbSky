/* SparkApp.scala */
package spark

import java.io.{FileWriter, BufferedWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder

import scala.util.Random

import probsky.method._

object ExperimentApp {

  def main(args: Array[String]) : Unit = {
    val log = LogManager.getRootLogger()
    log.setLevel(Level.FATAL)
    log.info("PSky")
    val conf = new SparkConf().setAppName("P-Sky").setMaster("local[*]")
     .set("spark.driver.memory", "32g")
     .set("spark.executor.memory","32g")
     .set("spark.memory.offHeap.enabled", "true")
     .set("spark.memory.offHeap.size","32g")
     .set("spark.kryo.registrationRequired", "true")
     .set("spark.kryo.registrator", "probsky.util.function.serializer.ProbSkyKryoRegistrator")
     .set("spark.kryoserializer.buffer.max","2047m")
     .set("spark.kryoserializer.buffer","256k")
    val sc = new SparkContext(conf)
    // ------------- Configuration  -------------
    val dimension = 7
    val lowerBoundProbTested = Array(0.6)
    val numPartitionTested = Array(8)
    // ------------- ProbSky  -------------
    val splitThresholdForQuadtree = 5
    val chunkSize = 2000
    val numDominatingInstances = 5000
    val numDominatingCorner = 1000

    // ------------- PS-QPF-MR -------------
    // ------------- PS-BRF-MR -------------
    val splitThreshold = 180
    val sampleSize = 1000
    val heapCapacity = 100

    val projectDir = System.getProperty("user.dir")
    val queryMethodTested = Array("PSky")
    for {
      i <- 0 until 1
      lowerBoundProb <- lowerBoundProbTested
      numPartition <- numPartitionTested
    }
    {
      val randSeed: Long = System.nanoTime
      val srcFolder = f"$projectDir/data/dataset/"
      val scrName = "cor_7d_200000_10.txt"
      val srcFile = f"$srcFolder$scrName"
      val srcData = sc.textFile(srcFile, numPartition)
      val numInstance = srcData.count
      for (queryMethod <- queryMethodTested) {
        val strBuilder = new StringBuilder()
        val startTime = System.nanoTime()
        val sparkQuery = queryMethod match {
          case "PSky-Base" => new PSkyBase(sc, srcData, numPartition, dimension, lowerBoundProb, numDominatingCorner, chunkSize, randSeed)
          case "PSky" => new PSky(sc, srcData, numPartition, dimension, lowerBoundProb, numDominatingCorner, numDominatingInstances, chunkSize, splitThresholdForQuadtree, randSeed)
          case "PSky-No-Grid" => new PSkyNoGrid(sc, srcData, numPartition, dimension, lowerBoundProb, numDominatingCorner, numDominatingInstances, chunkSize, splitThresholdForQuadtree, randSeed)
          case "PSky-No-Pivot" => new PSkyNoPivot(sc, srcData, numPartition, dimension, lowerBoundProb, numDominatingCorner, numDominatingInstances, chunkSize, splitThresholdForQuadtree, randSeed)
          case "PSky-No-DIP" => new PSky(sc, srcData, numPartition, dimension, lowerBoundProb, numDominatingCorner, 0, chunkSize, splitThresholdForQuadtree, randSeed)
          //case "PSBRFMR" => new PSBRFMR(sc, srcData, numPartition, dimension, lowerBoundProb, sampleSize, splitThreshold)
          //case "PSQPFMR" => new PSQPFMR(sc, srcData, numPartition, dimension, lowerBoundProb, sampleSize, splitThreshold, heapCapacity)
        }
        val candObjs = sparkQuery.run().sortBy( o => (o._2, o._1))
        val runningTime = (System.nanoTime() - startTime) / 1e9
        strBuilder ++= "------ Candidate Objects ------\n"
        /*
				// print out all the candidate objects with their skyline probabilities.
        if (candObjs != null) {
          candObjs.foreach{
            case (objId, objSkyProb) => {
              strBuilder ++= objId + " = " + objSkyProb + "\n"
            }
          }
        }
        */

        val summary = ("| File: " + srcFile
          + "\n | Method: " + queryMethod
          + "\n | Instance: " + numInstance
          + "\n | Partition: " + numPartition
          + "\n | Probability: " + lowerBoundProb
          + "\n | splitThresholdForQuadtree: " + splitThresholdForQuadtree
          + "\n | numDominatingInstances: " + numDominatingInstances
          + "\n | numDominatingCorner: " + numDominatingCorner
          + "\n | Random Seed: " + randSeed
          + "\n | Candidate Size: " + candObjs.size
          + "\n | Elapsed time: " + runningTime + " s\n")
        strBuilder ++= summary

        val content = strBuilder.toString
        val folder = "experiment/"
        val fileName = f"$scrName%s_$queryMethod%s.txt"
        val filePath = folder + fileName
        val w = new BufferedWriter(new FileWriter(filePath, true))
        w.write(content)
        w.close()
        println(content)
      }
      println()
      Thread.sleep(1000L)
    }
    sc.stop()
  }
}
