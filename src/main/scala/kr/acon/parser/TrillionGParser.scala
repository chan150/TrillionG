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

package kr.acon.parser

class TrillionGParser extends Parser {
  var logn = 20
  var ratio = 16
  var a = 0.57d
  var b = 0.19d
  var c = 0.19d
  var d = 0.05d

  def n = Math.pow(2, logn).toLong

  def e = ratio * n

  def isNSKG = (noise != 0d)

  def param = (a, b, c, d)

  var noise = 0.0d
  var opt = 0

  val termTrillionG = List("-logn", "-ratio", "-p", "-n", "-r", "-noise", "-opt")

  override val term = termBasic.++(termTrillionG)

  override def setParameters(name: String, value: String) {
    super.setParameters(name, value)
    name match {
      case "logn" | "n" => logn = value.toInt
      case "ratio" | "r" => ratio = value.toInt
      case "noise" => noise = value.toDouble
      case "p" => {
        val split = value.trim.split(",")
        a = split(0).toDouble
        if (split.length == 1) {
          b = a / 3.0
          c = a / 3.0
        }
        if (split.length >= 3) {
          b = split(1).toDouble
          c = split(2).toDouble
        }
        d = 1 - a - b - c
      }
      case "opt" => opt = value.toInt
      case _ => // important to set
    }
  }
}
