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

package kr.acon

import kr.acon.generator.skg.SKGGenerator

object ApplicationMain {
  def main(args: Array[String]): Unit = {
    val apps = Seq("TrillionG", "TrillionBA", "EvoGraph", "EvoGraphPlus", "TGSim")
    require(args.length >= 1, s"argument must be larger than 1, args=${args.deep}")
    require(apps.contains(args(0)), s"Unknown application, " +
      s"please set application type in [${apps.mkString(", ")}]")

    val remainArgs = args.slice(1, args.length)
    println(s"Launching ${args(0)}...")
    args(0) match {
      case "TrillionG" => SKGGenerator(remainArgs)
      case _ =>
    }
  }
}