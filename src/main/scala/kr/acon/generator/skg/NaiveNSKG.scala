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

class NaiveNSKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int, noise: Double)
    extends SKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int) {
  private[generator] def this(p: TrillionGParser) {
    this(p.a, p.b, p.c, p.d, p.logn, p.ratio, p.noise)
  }
  
  @inline protected val c_ad = 2 / (a + d)
  @inline protected val c_c = 1 / c
  @inline protected val c_adadab = (a - d) / ((a + d) * (a + b))
  @inline protected val c_adadcd = (a - d) / ((a + d) * (c + d))

  @inline protected val kmu = Array.tabulate(logn)(x => -noise + 2 * noise * math.random)

  @inline protected final def getPoutWithoutNoise(vid: Long) = super.getPout(vid)
  
  @inline private[generator] override def getPout(vid: Long) = {
    val bs = bitSum(vid)
    var tab = 1d
    var x = 0
    while (x < logn) {
      val t = if ((vid >>> x) % 2 == 0) {
        1d - c_adadab * kmu(x)
      } else {
        1d + c_adadcd * kmu(x)
      }
      tab *= t
      x += 1
    }
    abcd(bs) * tab
  }
  
  @inline protected final def getCDFWithoutNoise(vid: Long, logto: Int) = super.getCDF(vid, logto)

  @inline private[generator] override def getCDF(vid: Long, logto: Int) = {
    val bs = bitSum(vid >>> logto)
    val aab = this.aab(logn - logto - bs)
    val ccd = this.ccd(bs)
    var tab = 1d
    var x = 0
    while (x < logn) {
      val t = if (x < logn - logto) {
        if ((vid >>> x) % 2 == 0) {
          1d - c_ad * kmu(x)
        } else {
          1d + c_c * kmu(x)
        }
      } else {
        if ((vid >>> x) % 2 == 0) {
          1d - c_adadab * kmu(x)
        } else {
          1d + c_adadcd * kmu(x)
        }
      }
      tab *= t
      x += 1
    }
    getPout(vid) * aab * ccd * tab
  }
}