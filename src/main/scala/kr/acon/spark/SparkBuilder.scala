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

package kr.acon.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SparkBuilder {
  private val configMap = mutable.Map[String, String]()
  private var master = "local[*]"
  private var appName = "Application"

  private var builder: SparkSession.Builder = null
  var spark: SparkSession = null

  def setConfig(key: String, value: String): Unit = {
    configMap.put(key, value)
  }

  def setAppName(_appName: String): Unit = {
    appName = _appName
  }

  def setMaster(_master: String): Unit = {
    master = _master
  }

  def build() = {
    try {
      builder = SparkSession.builder().appName(appName)
      configMap.foreach {
        case (key, value) =>
        builder = builder.config(key, value)
      }
      spark = builder.getOrCreate()
    } catch {
      case _: SparkException => {
        builder = SparkSession.builder().appName(appName).master("local[*]")
        configMap.foreach {
          case (key, value) =>
            builder = builder.config(key, value)
        }
        spark = builder.getOrCreate()
      }
    }
    spark
  }
}