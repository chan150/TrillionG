package kr.acon.generator

import java.util.Date

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import kr.acon.util.Parser
import kr.acon.util.Predef

class BaseGenerator extends Serializable {
  val parser = new Parser
  val appName = "Base Graph Generator"

  def run(sc: SparkContext): RDD[(Long, LongOpenHashBigSet)] = {
    println("do nothing")
    null
  }

  def apply(implicit args: Array[String] = new Array[String](0),
            f: (RDD[(Long, LongOpenHashBigSet)], Parser) => Unit = Predef.plotOutDegree,
            isPrinted: Boolean = true) {
    val appNameArgs = appName + " / " + (new Date).toString + " / " + args.mkString(" ")
    val conf = new SparkConf().setAppName(appNameArgs)
    val sc = new SparkContext(conf)
    parser.argsParser(args)
    val startTime = new Date

    val edges = run(sc)

    if (edges != null) {
      if (parser.getOutputFormat != null) {
        writeEdges(edges)
      } else if (f != null) {
        f(edges, parser)
      } else {
        edges.count
      }
    }

    val endTime = new Date
    if (isPrinted) {
      println((endTime.getTime - startTime.getTime) / 1000f + " seconds spent.")
      parser.printDetailParameter
    }
    sc.stop
  } // end of apply

  def writeEdges(edges: RDD[(Long, LongOpenHashBigSet)]) {
    val path = parser.hdfs + parser.file
    val format = parser.getOutputFormat
    val codec = parser.getCompressCodec
    if (codec != null)
      edges.saveAsHadoopFile(path, classOf[LongWritable], classOf[LongOpenHashBigSet], format, codec)
    else
      edges.saveAsHadoopFile(path, classOf[LongWritable], classOf[LongOpenHashBigSet], format)
  }

  def largeVertices(sc: SparkContext) = {
    sc.range(0, parser.n - 1, 1, parser.machine)
  }
}

