package kr.acon.util

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.mapred.OutputFormat

import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import kr.acon.lib.io.ADJ6OutputFormat
import kr.acon.lib.io.ADJ8OutputFormat
import kr.acon.lib.io.CSR6OutputFormat
import kr.acon.lib.io.CSR8OutputFormat
import kr.acon.lib.io.TSVOutputFormat
import kr.acon.lib.io.CSR4OutputFormat
import kr.acon.lib.io.ADJ4OutputFormat

class Parser extends Serializable {
  var hdfs = "hdfs://jupiter01:9000/user/himchan/"
  var file = "graph"
  var machine = 120
  var logn = 20
  var ratio = 16
  var a = 0.57d
  var b = 0.19d
  var c = 0.19d
  var d = 0.05d
  def n = Math.pow(2, logn).toLong
  def e = ratio * n
  def param = (a, b, c, d)
  var noise = 0.0d
  var format = "adj"
  var compress = "NONE"
  var rng = System.currentTimeMillis
  var opt = 0
  def exactEdges = e 

  var threshold = Long.MaxValue
  val term = List("-logn", "-ratio", "-p", "-machine", "-n", "-r", "-m", "-hdfs", "-noise", "-format", "-compress", "-rng", "-opt", "-threshold","-output")

  def isNSKG = (noise != 0d)

  def argsParser(args: Array[String]) {
    var i = 0
    while (i < args.length) {
      if (term.contains(args(i))) {
        setParm(args(i).replaceFirst("-", ""), args(i + 1))
        i += 2
      } else {
        setParm("output", args(i))
        i += 1
      }
    }
  }

  def setParm(name: String, value: String) {
    name match {
      case "p" => {
        val split = value.trim.split(",")
        a = split(0).toDouble
        if (split.length == 1) {
          b = a / 3.0
          c = a / 3.0
        }
        if (split.length >= 3) {
          b = split(1).toDouble
          c = split(2).toDouble
        }
        d = 1 - a - b - c
      }
      case "hdfs" => hdfs = value
      case "logn" | "n" => logn = value.toInt
      case "ratio" | "r" => ratio = value.toInt
      case "machine" | "m" => machine = value.toInt
      case "output" => file = value
      case "noise" => noise = value.toDouble
      case "rng" => rng = value.toLong
      case "opt" => opt = value.toInt
      case "threshold" => threshold = value.toLong
      case "format" => {
        if (List("tsv", "adj", "csr").contains(value.toLowerCase))
          format = value.toLowerCase
        else if ("NONE" == value.toUpperCase)
          format = value.toUpperCase
        else
          format = value
      }
      case "compress" => {
        compress = value
      }
    }
  }

  def printDetailParameter {
    println("Param=%s, |V|=%d (%d), |E|=%d, Noise=%.3f".format(param.toString, n, logn, exactEdges, noise))
    println("PATH=%s, Machine=%d".format(hdfs + file, machine))
    println("OutputFormat=%s, CompressCodec=%s".format(format.toString(), compress.toString()))
    println("RandomSeed=%d".format(rng))
  }

  def getOutputFormat: Class[_ <: OutputFormat[Long, LongOpenHashSet]] = {
    var outputFormatClass: Any = null
    if (format.startsWith("adj")) {
      if (format == "adj" || format == "adj6") {
        outputFormatClass = classOf[ADJ6OutputFormat] 
      } else if (format == "adj4") {
        outputFormatClass = classOf[ADJ4OutputFormat]
      } else if (format == "adj8") {
        outputFormatClass = classOf[ADJ8OutputFormat]
      }
    } else if (format == "tsv")
      outputFormatClass = classOf[TSVOutputFormat]
    else if (format.startsWith("csr")) {
      if (format == "csr" || format == "csr6") {
        outputFormatClass = classOf[CSR6OutputFormat]
      } else if (format == "csr4") {
        outputFormatClass = classOf[CSR4OutputFormat]
      } else if (format == "csr8") {
        outputFormatClass = classOf[CSR8OutputFormat]
      }
    } else if (format == "NONE") {
      outputFormatClass = null
    } else {
      try {
        outputFormatClass = Class.forName(format).asInstanceOf[Class[OutputFormat[Long, LongOpenHashSet]]]
      } catch {
        case _: Throwable => {
          println("Mismatch output format class")
          outputFormatClass = null
        }
      }
    }
    outputFormatClass.asInstanceOf[Class[OutputFormat[Long, LongOpenHashSet]]]
  }

  def getCompressCodec: Class[_ <: CompressionCodec] = {
    var compressCodecClass: Any = null
    if (compress == "NONE") {
      compressCodecClass = null
    } else if (compress == "snappy") {
      compressCodecClass = classOf[SnappyCodec]
    } else if (compress == "bzip" || compress == "bzip2") {
      compressCodecClass = classOf[BZip2Codec]
    } else {
      try {
        compressCodecClass = Class.forName(compress).asInstanceOf[Class[CompressionCodec]]
      } catch {
        case _: Throwable => {
          println("Mismatch compression codec")
          compressCodecClass = null
        }
      }
    }
    compressCodecClass.asInstanceOf[Class[CompressionCodec]]
  }
}