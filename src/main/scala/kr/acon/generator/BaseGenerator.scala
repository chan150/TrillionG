/*
 *      __________  ______    __    ________  _   __   ______
 *     /_  __/ __ \/  _/ /   / /   /  _/ __ \/ | / /  / ____/
 *      / / / /_/ // // /   / /    / // / / /  |/ /  / / __
 *     / / / _, _// // /___/ /____/ // /_/ / /|  /  / /_/ /
 *    /_/ /_/ |_/___/_____/_____/___/\____/_/ |_/   \____/
 *
 *    Copyright (C) 2017 Himchan Park (chan150@dgist.ac.kr)
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package kr.acon.generator

import java.util.Date

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import kr.acon.parser.Parser
import kr.acon.spark.SparkBuilder
import kr.acon.util.LoggingPolicy
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

class BaseGenerator extends Serializable {
  LoggingPolicy.disableLogging

  lazy val spark = SparkBuilder.build()
  def sc = spark.sparkContext

  val parser = new Parser
  val appName = "Base Graph Generator"
  var edges: RDD[(Long, LongOpenHashBigSet)] = null

  def run: RDD[(Long, LongOpenHashBigSet)] = {
    println("do nothing")
    null
  }

  def postProcessing() = {
    println("do nothing")
  }

  def apply(implicit args: Array[String] = new Array[String](0)) {
    parser.argsParser(args)
    SparkBuilder.setAppName(appName + " / " + (new Date).toString + " / " + args.deep)

    val startTime = new Date

    edges = run
    if (edges != null) {
      if (parser.getFunctionClass != null) {
        parser.getFunctionClass.newInstance().f(edges, parser)
      } else {
        if (parser.getOutputFormat != null) {
          writeEdges
        } else {
          edges.count
        }
      }
    }

    val endTime = new Date

    println((endTime.getTime - startTime.getTime) / 1000f + " seconds spent.")
    postProcessing()
    sc.stop
  } // end of apply

  def writeEdges() {
    val path = parser.hdfs + parser.file
    val format = parser.getOutputFormat
    val codec = parser.getCompressCodec
    if (codec != null)
      edges.saveAsHadoopFile(path, classOf[LongWritable], classOf[LongOpenHashBigSet], format, codec)
    else
      edges.saveAsHadoopFile(path, classOf[LongWritable], classOf[LongOpenHashBigSet], format)
  }
}

