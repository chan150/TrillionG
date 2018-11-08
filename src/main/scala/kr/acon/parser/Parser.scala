/*
 *
 *       __________  ______    __    ________  _   __   ______
 *      /_  __/ __ \/  _/ /   / /   /  _/ __ \/ | / /  / ____/
 *       / / / /_/ // // /   / /    / // / / /  |/ /  / / __
 *      / / / _, _// // /___/ /____/ // /_/ / /|  /  / /_/ /
 *     /_/ /_/ |_/___/_____/_____/___/\____/_/ |_/   \____/
 *
 *     Copyright (C) 2017 Himchan Park (chan150@dgist.ac.kr)
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package kr.acon.parser

import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import kr.acon.io._
import kr.acon.util.PredefinedFunctions
import org.apache.hadoop.io.compress.{BZip2Codec, CompressionCodec, SnappyCodec}
import org.apache.hadoop.mapred.OutputFormat

class Parser extends Serializable {
  var master = ""
  var hdfs = "./"
  var file = "graph"
  var machine = 120
  var format = "adj"
  var compress = "none"
  var function = "none"

  // plot option
  var xy = "xy"

  // seed
  var rng = System.currentTimeMillis

  //  // BA model (TrillionBA)
  //  var ban = 1000000l
  //  var bam = 2
  //  var bam0 = 2
  //  var bal = 10000l

  val termBasic = List("-machine", "-m", "-hdfs", "-format", "-compress", "-rng", "-output", "-function", "-f")
  //  val termTrillionBA = List("-ba.n", "-ba.m", "-ba.m0", "-ba.l")

  val term = termBasic

  def argsParser(args: Array[String]) {
    var i = 0
    while (i < args.length) {
      if (term.contains(args(i))) {
        setParameters(args(i).replace("-", ""), args(i + 1))
        i += 2
      } else {
        setParameters("output", args(i))
        i += 1
      }
    }
  }

  def setParameters(name: String, value: String) {
    name match {
      case "hdfs" => hdfs = value
      case "machine" | "m" => machine = value.toInt
      case "output" => file = value
      case "rng" => rng = value.toLong
      case "format" => format = value
      case "compress" => compress = value
      case "function" | "f" => function = value
      case _ => // important to set

      //      case "ba.n" => ban = value.toLong
      //      case "ba.m" => bam = value.toInt
      //      case "ba.m0" => bam0 = value.toInt
      //      case "ba.l" => bal = value.toInt
    }
  }

  def getFunctionClass: Class[_ <: PredefinedFunctions.Function] = {
    var functionClass: Any = null

    if (function.startsWith("count")) {
      functionClass = classOf[PredefinedFunctions.COUNT]
    } else if (function.startsWith("out")) {
      functionClass = classOf[PredefinedFunctions.OUT]
    } else if (function.startsWith("in")) {
      functionClass = classOf[PredefinedFunctions.IN]
    } else if (function == "undirected") {
      functionClass = classOf[PredefinedFunctions.UNDIRECTED]
    } else if (function == "both") {
      functionClass = classOf[PredefinedFunctions.BOTH]
    } else if (function == "parquet") {
      functionClass = classOf[PredefinedFunctions.PARQUET]
    } else if (function == "none") {
      functionClass = null
    } else {
      try {
        functionClass = Class.forName(function)
          .asInstanceOf[Class[PredefinedFunctions.Function]]
      } catch {
        case _: Throwable => {
          println("Mismatch function class")
          functionClass = null
        }
      }
    }

    functionClass.asInstanceOf[Class[_ <: PredefinedFunctions.Function]]
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
    } else if (format == "none") {
      outputFormatClass = null
    } else {
      try {
        outputFormatClass = Class.forName(format)
          .asInstanceOf[Class[OutputFormat[Long, LongOpenHashSet]]]
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
    if (compress == "none") {
      compressCodecClass = null
    } else if (compress == "snappy") {
      compressCodecClass = classOf[SnappyCodec]
    } else if (compress == "bzip" || compress == "bzip2") {
      compressCodecClass = classOf[BZip2Codec]
    } else {
      try {
        compressCodecClass = Class.forName(compress)
          .asInstanceOf[Class[CompressionCodec]]
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