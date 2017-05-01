package org.apache.spark.graphx

import org.apache.spark.graphx.impl.EdgePartitionBuilder
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet

object DirectGraphLoader extends Logging {
  def edgeListFile(e: RDD[(Long, LongOpenHashBigSet)]): Graph[Int, Int] =
    {
      val canonicalOrientation: Boolean = false
      val numEdgePartitions: Int = -1
      val edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
      val vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY

      val startTime = System.currentTimeMillis

      // Parse the edge data table directly into edge partitions
      val lines = e.flatMap { case (vid, adj) => adj.toLongArray().map(dst => vid + "\t" + dst) }
      val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
        val builder = new EdgePartitionBuilder[Int, Int]
        iter.foreach { line =>
          if (!line.isEmpty && line(0) != '#') {
            val lineArray = line.split("\\s+")
            if (lineArray.length < 2) {
              throw new IllegalArgumentException("Invalid line: " + line)
            }
            val srcId = lineArray(0).toLong
            val dstId = lineArray(1).toLong
            if (canonicalOrientation && srcId > dstId) {
              builder.add(dstId, srcId, 1)
            } else {
              builder.add(srcId, dstId, 1)
            }
          }
        }
        Iterator((pid, builder.toEdgePartition))
      }.persist(edgeStorageLevel).setName("GraphLoader.edgeListFile - edges")
      edges.count()

      logInfo("It took %d ms to load the edges".format(System.currentTimeMillis - startTime))

      GraphImpl.fromEdgePartitions(edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
        vertexStorageLevel = vertexStorageLevel)
    } // end of edgeListFile
}
