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

package kr.acon.io.recordwriter

import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.util.Arrays

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet

abstract class ADJRecordWriter(out: DataOutputStream) extends BaseRecordWriter(out) {
  def byteAlign: Int
  def byteAlgignPut(buffer: ByteBuffer, offset: Int, v: Long): Unit
  
  @inline final def writeElement(v: Long) {
    val buffer = ByteBuffer.allocate(byteAlign)
    byteAlgignPut(buffer, 0, v)
    val buf = buffer.array
    synchronized {
      out.write(buf)
    }
  }

  @inline final def writeElements(value: LongOpenHashBigSet) = {
    val array = value.toLongArray
    Arrays.sort(array)
    val iter = array.iterator
    val NumIter = (value.size64 / (Int.MaxValue.toLong * byteAlign * 2)).toInt
    for (i <- 0 until NumIter) {
      val bufferSize = Integer.MAX_VALUE / 2
      val buffer = ByteBuffer.allocate(bufferSize)
      var offset = 0
      while (iter.hasNext && offset < bufferSize) {
        val v = iter.next
        byteAlgignPut(buffer, offset, v)
        offset += byteAlign
      }
      val buf = buffer.array
      synchronized {
        out.write(buf)
      }
    }

    val reminder = (value.size64 % (Int.MaxValue.toLong * byteAlign * 2)).toInt
    if (reminder != 0) {
      val buffer = ByteBuffer.allocate(byteAlign * reminder)
      var offset = 0;
      while (iter.hasNext) {
        val v = iter.next
        byteAlgignPut(buffer, offset, v)
        offset += byteAlign
      }
      val buf = buffer.array
      synchronized {
        out.write(buf)
      }
    }
  }

  override def write(key: Long, value: LongOpenHashBigSet) = {
    writeElement(key)
    writeElement(value.size64)
    writeElements(value)
  }
}

@inline class ADJ4RecordWriter(out: DataOutputStream) extends ADJRecordWriter(out) {
  @inline final override def byteAlign = 4
  @inline final override def byteAlgignPut(buffer: ByteBuffer, offset: Int, v: Long) = {
    buffer.putInt(offset, v.toInt)
  }
}

@inline class ADJ6RecordWriter(out: DataOutputStream) extends ADJRecordWriter(out) {
  @inline final override def byteAlign = 6
  @inline final override def byteAlgignPut(buffer: ByteBuffer, offset: Int, v: Long) = {
    buffer.putShort(offset, (v >>> 32).toShort)
    buffer.putInt(offset + 2, v.toInt)
  }
}

@inline class ADJ8RecordWriter(out: DataOutputStream) extends ADJRecordWriter(out) {
  @inline final override def byteAlign = 8
  @inline final override def byteAlgignPut(buffer: ByteBuffer, offset: Int, v: Long) = {
    buffer.putLong(offset, v)
  }
}