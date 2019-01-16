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

package kr.acon.generator.skg

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import kr.acon.generator.BaseGenerator
import kr.acon.parser.TrillionGParser
import kr.acon.util.Utilities.RangePartitionFromDegreeRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object SKGGenerator extends BaseGenerator {
  override val appName = "TrillionG: A Trillion-scale Synthetic Graph Generator using a Recursive Vector Model"

  override val parser = new TrillionGParser

  implicit class RecVecGenClass(self: RDD[Long]) extends Serializable {
    def doRecVecGen(ds: Broadcast[_ <: SKG], rng: Long) = {
      self.mapPartitions {
        case partitions =>
          val skg = ds.value
          partitions.flatMap {
            case u =>
              val random = new SKG.randomClass(rng + u)
              val degree = skg.getDegree(u, random)
              if (degree < 1)
                Iterator.empty
              else {
                val recVec = skg.getRecVec(u)
                val sigmas = skg.getSigmas(recVec)
                val p = recVec.last
                val adjacency = new LongOpenHashBigSet(degree)
                var i = 0
                while (i < degree) {
                  adjacency.add(skg.determineEdge(u, recVec, sigmas, random))
                  i += 1
                }
                Iterator((u, adjacency))
              }
          }
      }
    }
  }

  override def postProcessing(): Unit = {
    println("Param=%s, |V|=%d (2^%d), |E|=%d, Noise=%.3f".format(parser.param, parser.n, parser.logn, parser.e, parser.noise))
    println("PATH=%s, Machine=%d".format(parser.hdfs + parser.file, parser.machine))
    println("OutputFormat=%s, CompressCodec=%s".format(parser.format, parser.compress))
    println("RandomSeed=%d".format(parser.rng))
  }

  override def run: RDD[(Long, LongOpenHashBigSet)] = {
    val vertexRDD = sc.range(0, parser.n - 1, 1, parser.machine)
    val ds = sc.broadcast(SKG.constructFrom(parser))
    val degreeRDD = vertexRDD.map(vid => (vid, ds.value.getExpectedDegree(vid)))
    val partitionedVertexRDD = degreeRDD.rangePartition(parser.machine, parser.n, parser.e)
    val edges = partitionedVertexRDD.doRecVecGen(ds, parser.rng)
    edges
  }
}