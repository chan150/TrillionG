/*
 *    Copyright 2017 Himchan Park
 *   __________  ______    __    ________  _   __   ______
 *  /_  __/ __ \/  _/ /   / /   /  _/ __ \/ | / /  / ____/
 *   / / / /_/ // // /   / /    / // / / /  |/ /  / / __
 *  / / / _, _// // /___/ /____/ // /_/ / /|  /  / /_/ /
 * /_/ /_/ |_/___/_____/_____/___/\____/_/ |_/   \____/
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

package kr.acon.lib.io.recordwriter

import java.io.DataOutputStream
import java.nio.ByteBuffer

import it.unimi.dsi.fastutil.longs.LongBigArrays
import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet

class TSVRecordWriter(out: DataOutputStream) extends BaseRecordWriter(out) {
  @inline final val utf8 = "UTF-8";

  @inline final val newline = "\n".getBytes(utf8)
  @inline final val keyValueSeparator = "\t".getBytes(utf8)

  @inline override def write(key: Long, value: LongOpenHashBigSet) = {
    val iter = value.iterator()
    val k = key.toString.getBytes(utf8)
    while (iter.hasNext) {
      val v = iter.next.toString.getBytes(utf8)
      synchronized {
        out.write(k)
        out.write(keyValueSeparator)
        out.write(v)
        out.write(newline)
      }
    }
  }
}