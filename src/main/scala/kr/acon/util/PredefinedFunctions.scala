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

package kr.acon.util

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import kr.acon.parser.Parser
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

object PredefinedFunctions {

  abstract class Function {
    type t1 = RDD[(Long, LongOpenHashBigSet)]
    type t2 = Parser

    def f(e: t1, parser: t2)

  }

  private def plotDegree(filename: String, logScaleAxis: String, multipleData: Array[String]*) {
    import scala.sys.process._
    createDatafile(s"${filename}", logScaleAxis, multipleData: _*)
    createScript(s"${filename}", logScaleAxis, multipleData: _*)
    s"gnuplot ${filename}.plt" !;
  }

  private def createDatafile(filename: String, logScaleAxis: String, multipleData: Array[String]*) {
    import java.io._
    multipleData.zipWithIndex.foreach {
      case (data, index) =>
        val writer = new PrintWriter(new FileOutputStream(s"${filename}.${index}.dat", false))
        data.foreach(x =>
          writer.println(x)
        )
        writer.close()
    }
  }

  private def createScript(filename: String, logScaleAxis: String, multipleData: Array[String]*) {
    import java.io._
    val writer = new PrintWriter(new FileOutputStream(s"${filename}.plt", false))
    val plotLineInScript = multipleData.zipWithIndex.map {
      case (data, index) =>
        val dataFilename = s"${filename}.${index}.dat"
        s""""${dataFilename}" using 1:2 with points pointtype 5 pointsize 0.5"""
    }.mkString(", ")
    val formatInPlot = if (logScaleAxis != "none") s"""set format ${logScaleAxis} "10^{%L}""" else ""
    val logscaleInPlot = if (logScaleAxis != "none") s"""set logscale ${logScaleAxis}""" else ""
    val script =
      s"""
set terminal postscript eps enhanced  color size 3in,1.5in
set output '${filename}.eps'
set nokey
set yrange [0.75:*]
${formatInPlot}
${logscaleInPlot}
plot ${plotLineInScript}
      """.split("\\n+")
    script.foreach(x =>
      writer.println(x)
    )
    writer.close()
  }

  class PARQUET extends Function {
    override def f(e: t1, parser: t2) {
      val sparkSession = SparkSession.getActiveSession.get
      import sparkSession.implicits._
      e.flatMap { case (v, adj) => new Iterator[(Long, Long)] {
        val it = adj.iterator()

        override def hasNext: Boolean = it.hasNext

        override def next(): (Long, Long) = (v, it.nextLong())
      }
      }.toDF("src", "dst").write.parquet(parser.hdfs + parser.file)
    }
  }

  class COUNT extends Function {
    override def f(e: t1, parser: t2) {
      e.count
    }
  }

  class NONE extends Function {
    override def f(e: t1, parser: t2) {
      println("do nothing")
    }
  }

  class OUT extends Function {
    override def f(e: t1, parser: t2) {
      val d = e.flatMap { x =>
        Iterator((x._1, x._2.size64()))
      }.reduceByKey(_ + _).
        map(x => (x._2, 1L)).reduceByKey(_ + _).
        map(x => s"${x._1}\t${x._2}").collect
      plotDegree(parser.file, parser.xy, d)
    }
  }

  class IN extends Function {
    override def f(e: t1, parser: t2) {
      val d = e.flatMap(x => x._2.toLongArray.
        map { y => (y, 1L) }).reduceByKey(_ + _).
        map(x => (x._2, 1L)).reduceByKey(_ + _).
        map(x => s"${x._1}\t${x._2}").collect
      plotDegree(parser.file, parser.xy, d)
    }
  }

  class BOTH extends Function {
    override def f(e: t1, parser: t2): Unit = {
      val ec = e.cache()
      (new IN).f(ec, parser)
      (new OUT).f(ec, parser)
    }
  }

  class UNDIRECTED extends Function {
    override def f(e: t1, parser: t2): Unit = {
      val d = e.flatMap { x =>
        x._2.toLongArray.map { y =>
          (y, 1L)
        }.++(List((x._1, x._2.size64())))
      }.reduceByKey(_ + _).map(x => (x._2, 1L)).
        reduceByKey(_ + _).map(x => s"${x._1}\t${x._2}").collect

      plotDegree(parser.file, parser.xy, d)
    }
  }

}