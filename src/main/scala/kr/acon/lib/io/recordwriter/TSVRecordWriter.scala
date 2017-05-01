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