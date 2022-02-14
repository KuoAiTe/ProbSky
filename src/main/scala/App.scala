package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.io.{FileWriter, BufferedWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import probsky.method._


object App {
  def main(args: Array[String]) : Unit = {
    val arglist = args.toList
    type OptionMap = Map[String, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--method" :: value :: tail => nextOption(map ++ Map("method" -> value), tail)
        case "--src" :: value :: tail => nextOption(map ++ Map("src" -> value), tail)
        case "--dimension" :: value :: tail => nextOption(map ++ Map("dimension" -> value.toInt), tail)
        case "--num-partition" :: value :: tail => nextOption(map ++ Map("num-partition" -> value.toInt), tail)
        case "--probability-threshold" :: value :: tail => nextOption(map ++ Map("probability-threshold" -> value.toDouble), tail)
        case "--num-dominating-corner" :: value :: tail => nextOption(map ++ Map("num-dominating-corner" -> value.toInt), tail)
        case "--bin-size" :: value :: tail => nextOption(map ++ Map("bin-size" -> value.toInt), tail)
        case "--num-dominating-instance" :: value :: tail => nextOption(map ++ Map("num-dominating-instance" -> value.toInt), tail)
        case "--split-threshold" :: value :: tail => nextOption(map ++ Map("split-threshold" -> value.toInt), tail)
        case "--sample-size" :: value :: tail => nextOption(map ++ Map("sample-size" -> value.toInt), tail)
        case "--heap-capacity" :: value :: tail => nextOption(map ++ Map("heap-capacity" -> value.toInt), tail)
        case "--rand-seed" :: value :: tail => nextOption(map ++ Map("rand-seed" -> value.toLong), tail)
        case option :: tail => nextOption(map, tail)
      }
    }
    val options = nextOption(Map(), arglist)
    println(options)

    val method = if (options.contains("method"))  options("method").asInstanceOf[String] else ""
    val src = if (options.contains("src"))  options("src").asInstanceOf[String] else ""
    val dimension = if (options.contains("dimension"))  options("dimension").asInstanceOf[Int] else -1
    val probabilityThreshold = if (options.contains("probability-threshold"))  options("probability-threshold").asInstanceOf[Double] else -1.0
    val numPartition = if (options.contains("num-partition"))  options("num-partition").asInstanceOf[Int] else -1
    val numDominatingCorner = if (options.contains("num-dominating-corner"))  options("num-dominating-corner").asInstanceOf[Int] else -1
    val binSize = if (options.contains("bin-size"))  options("bin-size").asInstanceOf[Int] else -1
    val numDominatingInstances = if (options.contains("num-dominating-instance"))  options("num-dominating-instance").asInstanceOf[Int] else -1
    val splitThreshold = if (options.contains("split-threshold"))  options("split-threshold").asInstanceOf[Int] else -1
    val sampleSize = if (options.contains("sample-size"))  options("sample-size").asInstanceOf[Int] else -1
    val heapCapacity = if (options.contains("heap-capacity"))  options("heap-capacity").asInstanceOf[Int] else -1
    val randSeed = if (options.contains("rand-seed"))  options("rand-seed").asInstanceOf[Long] else -1
    val filename = src.split("/").takeRight(1).mkString
    val appName = s"${method} | P = ${probabilityThreshold} - M = ${numPartition} | ${filename}"
    val format = new SimpleDateFormat("aa hh:mm:ss MMM dd yyyy")
    val startDate = format.format(Calendar.getInstance().getTime())
    val conf = new SparkConf()
    .setAppName(appName)
    val sc = new SparkContext(conf)
    val log = LogManager.getRootLogger()
    log.setLevel(Level.DEBUG)
    val srcData = sc.textFile(src, numPartition)
    val strBuilder = new StringBuilder()
    strBuilder ++= (
      "\n-------------------------------------------------------------------------------"
      + "\n | File: " + src
      + "\n | Method: " + method
      + "\n | Partition: " + numPartition
      + "\n | Lower Bound Prob: " + probabilityThreshold
    )
    val sparkQuery = method match {
      case "PSky-Base" => new PSkyBase(sc, srcData, numPartition, dimension, probabilityThreshold, numDominatingCorner, binSize, randSeed)
      case "PSky" => new PSky(sc, srcData, numPartition, dimension, probabilityThreshold, numDominatingCorner, numDominatingInstances, binSize, splitThreshold, randSeed)
      case "PSky-No-Grid" => new PSkyNoGrid(sc, srcData, numPartition, dimension, probabilityThreshold, numDominatingCorner, numDominatingInstances, binSize, splitThreshold, randSeed)
      case "PSky-No-Pivot" => new PSkyNoPivot(sc, srcData, numPartition, dimension, probabilityThreshold, numDominatingCorner, numDominatingInstances, binSize, splitThreshold, randSeed)
      case "PSky-No-DIP" => new PSky(sc, srcData, numPartition, dimension, probabilityThreshold, numDominatingCorner, 0, binSize, splitThreshold, randSeed)
      //case "PSQPFMR" => new PSQPFMR(sc, srcData, numPartition, dimension, probabilityThreshold, sampleSize, splitThreshold, heapCapacity)
      //case "PSBRFMR" => new PSBRFMR(sc, srcData, numPartition, dimension, probabilityThreshold, sampleSize, splitThreshold)
    }
    if (numDominatingInstances != -1) strBuilder ++= s"\n | Dominating Instances: $numDominatingInstances"
    if (numDominatingCorner != -1) strBuilder ++= s"\n | Dominating Corner: $numDominatingCorner"
    if (binSize != -1) strBuilder ++= s"\n | Chunk Size: $binSize"
    if (randSeed != -1) strBuilder ++= s"\n | Random Seed: $randSeed"
    if (sampleSize != -1) strBuilder ++= s"\n | Sample Size: $sampleSize"
    if (splitThreshold != -1) strBuilder ++= s"\n | Split Threshold: $splitThreshold"
    if (heapCapacity != -1) strBuilder ++= s"\n | Heap Capacity: $heapCapacity"
    val startTime = System.nanoTime
    val candObjs = sparkQuery.run().sortBy( o => (o._2, o._1))
    val endDate = format.format(Calendar.getInstance().getTime())
    val runningTime = (System.nanoTime - startTime) / 1e9
    val candidateSize = candObjs.size

    strBuilder ++= (
      "\n | Candidate Size: " + candObjs.size
      + "\n | Elapsed time: " + runningTime + " s"
      + "\n-------------------------------------------------------------------------------\n"
    )
    val summary = strBuilder.toString
    log.debug(summary)
    println(summary)
    val filePath = "result.csv"
    val w = new BufferedWriter(new FileWriter(filePath, true))
    w.write(f"$startDate,$endDate,$src,$dimension,$probabilityThreshold,$method,$numPartition,$candidateSize,$runningTime%.3f,$numDominatingInstances,$numDominatingCorner,$splitThreshold,$binSize,$sampleSize,$splitThreshold,$heapCapacity,$randSeed\n")
    w.close()
    // the following line is added to monitor the resource usage.
    // sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(1000); i }.count()
    sc.stop()
  }
}
