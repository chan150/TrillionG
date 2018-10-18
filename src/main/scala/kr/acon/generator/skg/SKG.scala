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

import kr.acon.parser.TrillionGParser
import kr.acon.util.Utilities

import scala.annotation.tailrec
import scala.util.Random

object SKG {
  final def constructFrom(p: TrillionGParser) = {
    if (p.opt != 0)
      new OptionalSKG(p)
    else if (p.noise == 0)
      new SKG(p)
    else
      new NSKG(p)
  }

  @inline final type randomClass = Random
}

class SKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int) extends Serializable {
  @inline protected final val random = new SKG.randomClass

  @inline protected final val n = Math.pow(2, logn).toLong
  @inline protected final val e = ratio * n

  @inline protected final val abcd = Array.tabulate(logn + 1)(x => math.pow(a + b, logn - x) * math.pow(c + d, x))
  @inline protected final val aab = Array.tabulate(logn + 1)(x => math.pow(a / (a + b), x))
  @inline protected final val ccd = Array.tabulate(logn + 1)(x => math.pow(c / (c + d), x))

  private[generator] def this(p: TrillionGParser) {
    this(p.a, p.b, p.c, p.d, p.logn, p.ratio)
  }

  @inline protected final def bitSum(s: Long): Int = {
    java.lang.Long.bitCount(s)
  }

  private[generator] def getPout(vid: Long) = {
    val bs = bitSum(vid)
    abcd(bs)
  }

  private[generator] def getExpectedDegree(vid: Long) = {
    e * getPout(vid)
  }

  private[generator] def getDegree(vid: Long, r: SKG.randomClass = random) = {
    val s = getExpectedDegree(vid: Long)
    math.round(s + math.sqrt(s * (1 - getPout(vid))) * r.nextGaussian).toLong
  }

  private[generator] def getCDF(vid: Long, logto: Int) = {
    val bs = bitSum(vid >>> logto)
    val aab = this.aab(logn - logto - bs)
    val ccd = this.ccd(bs)
    getPout(vid) * aab * ccd
  }

  private[generator] final def getRecVec(vid: Long) = {
    val array = new Array[Double](logn + 1)
    var i = 0
    while (i <= logn) {
      array(i) = getCDF(vid, i)
      i += 1
    }
    array
  }

  @inline private[generator] final def getSigmas(recVec: Array[Double]) = {
    val array = new Array[Double](logn)
    var i = 0
    while (i < logn) {
      array(i) = recVec(i) / (recVec(i + 1) - recVec(i))
      i += 1
    }
    array
  }

  @inline
  @tailrec private[generator] final def determineEdge0BinarySearch(gp: Double, recVec: Array[Double], sigmas: Array[Double], prev: Int, acc: Long = 0): Long = {
    val k = Utilities.binarySearch(recVec, prev, gp)
    if (0 > k || k >= prev) {
      acc
    } else {
      val sigma = sigmas(k)
      val sp = sigma * (gp - recVec(k))
      determineEdge0BinarySearch(sp, recVec, sigmas, k + 1, (1l << (k)) + acc)
    }
  }

  private[generator] def determineEdge(u: Long, RecVec: Array[Double], sigmas: Array[Double], r: SKG.randomClass = random): Long = {
    val p = r.nextDouble * RecVec.last
    determineEdge0BinarySearch(p, RecVec, sigmas, logn)
  }
}