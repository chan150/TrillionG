package kr.acon.lib.io.recordwriter

import java.io.DataOutputStream

import org.apache.hadoop.mapred.RecordWriter
import org.apache.hadoop.mapred.Reporter

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet

abstract class BaseRecordWriter(out: DataOutputStream) extends RecordWriter[Long, LongOpenHashBigSet] {
  override def write(key: Long, value: LongOpenHashBigSet)
  override def close(reporter: Reporter) = synchronized {
    out.close()
  }
}