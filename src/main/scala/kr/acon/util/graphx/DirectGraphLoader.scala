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

package org.apache.spark.graphx

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
