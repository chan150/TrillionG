package kr.acon.generator.skg

import scala.Iterator

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import kr.acon.generator.BaseGenerator
import kr.acon.util.Util.RangePartitionFromDegreeRDD

object SKGGenerator extends BaseGenerator {
  override val appName = "TrillionG: A Trillion-scale Synthetic Graph Generator using a Recursive Vector Model"

  implicit class RecVecGenClass(self: RDD[Long]) extends Serializable {
    def doRecVecGen(bskg: Broadcast[_ <: SKG]) = {
      self.mapPartitions {
        case partitions =>
          val skg = bskg.value
          val random = new SKG.randomClass
          partitions.flatMap {
            case u =>
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

  override def run(sc: SparkContext): RDD[(Long, LongOpenHashBigSet)] = {
    val verticesRDD = largeVertices(sc)
    val bskg = sc.broadcast(SKG.constructFrom(parser))
    val degreeRDD = verticesRDD.map(vid => (vid, bskg.value.getExpectedDegree(vid)))
    val partitionedVertices = degreeRDD.rangePartition(parser.machine, parser.n, parser.e)
    val edges = partitionedVertices.doRecVecGen(bskg)
    edges
  }
}