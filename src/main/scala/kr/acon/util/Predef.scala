package kr.acon.util

import org.apache.spark.graphx.DirectGraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.storage.StorageLevel

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet

import sys.process._

object Predef {
  def count(e: RDD[(Long, LongOpenHashBigSet)], parser: Parser) {
    e.count
  }

  def cc(e: RDD[(Long, LongOpenHashBigSet)], parser: Parser) = {
    val graph = DirectGraphLoader.edgeListFile(e)
    graph.connectedComponents()
  }

  def plotDegree(parser: Parser) {
    "hadoop fs -copyToLocal " + parser.file + " temp"!;
    "gnuplot app/degree.plot"!;
    "rm -rf temp"!;
    "mv output.eps " + parser.file + ".eps"!;
	"okular " + parser.file + ".eps"!;
  }

  def plotOutDegree(e: RDD[(Long, LongOpenHashBigSet)], parser: Parser) {
    val d = e.map(x => (x._2.size64, 1L)).reduceByKey(_ + _).map(x => x._1 + "\t" + x._2)
    d.saveAsTextFile(parser.hdfs + parser.file)
    plotDegree(parser)
  }

  def plotInDegree(e: RDD[(Long, LongOpenHashBigSet)], parser: Parser) {
    val d = e.flatMap(x => x._2.toLongArray.map { y => (y, 1L) }).reduceByKey(_ + _)
      .map(x => (x._2, 1L)).reduceByKey(_ + _).map(x => x._1 + "\t" + x._2)
    d.saveAsTextFile(parser.hdfs + parser.file)
    plotDegree(parser)
  }

  def plotBothDegree(e: RDD[(Long, LongOpenHashBigSet)], parser: Parser) {
    val el = e.persist(StorageLevel.DISK_ONLY_2)
    val ori = parser.file
    parser.file = ori + "_out"
    plotOutDegree(el, parser)
    parser.file = ori + "_in"
    plotInDegree(el, parser)
  }
}