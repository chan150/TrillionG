package kr.acon.lib.io.recordwriter

import java.io.DataOutputStream
import java.nio.ByteBuffer

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
    val iter = value.iterator
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