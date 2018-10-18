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

class NSKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int, noise: Double)
    extends NaiveNSKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int, noise: Double) {
  private[skg] def this(p: TrillionGParser) {
    this(p.a, p.b, p.c, p.d, p.logn, p.ratio, p.noise)
  }

  @inline private[this] val len = math.ceil(logn / 16d).toInt
  @inline private[this] val c_pre1 = Array.tabulate(len, 1 << 16)(
    { (offset, vid) =>
      var tab = 1d
      var x = offset * 16
      while (x < math.min((offset + 1) * 16, logn)) {
        val t = if ((vid >>> (offset * 16) >>> x) % 2 == 0) {
          1d - c_adadab * kmu(x)
        } else {
          1d + c_adadcd * kmu(x)
        }
        tab *= t
        x += 1
      }
      tab
    })

  @inline private[generator] override def getPout(vid: Long) = {
    var tab = 1d
    var shift = 0
    while (shift < len) {
      tab *= c_pre1(shift)((vid << (48 - shift) >>> 48).toInt)
      shift += 16
    }
    getPoutWithoutNoise(vid) * tab
  }

  @inline private[this] val c_pre2 = Array.tabulate(len, logn + 1, 1 << 16)(
    { (offset, logto, vid) =>
      var tab = 1d
      var x = offset * 16
      while (x < math.min((offset + 1) * 16, logn)) {
        val t = if (x < logn - logto) {
          if ((vid >>> (offset * 16) >>> x) % 2 == 0) {
            1d - c_ad * kmu(x)
          } else {
            1d + c_c * kmu(x)
          }
        } else {
          if ((vid >>> (offset * 16) >>> x) % 2 == 0) {
            1d - c_adadab * kmu(x)
          } else {
            1d + c_adadcd * kmu(x)
          }
        }
        tab *= t
        x += 1
      }
      tab
    })

  @inline private[generator] override def getCDF(vid: Long, logto: Int) = {
    var tab = 1d
    var shift = 0
    while (shift < len) {
      tab *= c_pre2(shift)(logto)((vid << (48 - shift) >>> 48).toInt)
      shift += 16
    }
    getCDFWithoutNoise(vid, logto) * tab
  }
}