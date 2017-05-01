package kr.acon.lib.io

import java.io.DataOutputStream

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordWriter
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.util.ReflectionUtils

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet

abstract class BaseOutputFormat extends FileOutputFormat[Long, LongOpenHashBigSet] {
  @inline def getRecordWriter(out: DataOutputStream): RecordWriter[Long, LongOpenHashBigSet]

  @inline override def getRecordWriter(ignored: FileSystem,
                               job: JobConf,
                               name: String,
                               progress: Progressable) = {
    val isCompressed = FileOutputFormat.getCompressOutput(job)
    if (!isCompressed) {
      val file = FileOutputFormat.getTaskOutputPath(job, name)
      val fs = file.getFileSystem(job)
      val fileOut = fs.create(file, progress)
      getRecordWriter(fileOut)
    } else {
      val codecClass = FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
      val codec = ReflectionUtils.newInstance(codecClass, job)
      val file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension())
      val fs = file.getFileSystem(job)
      val fileOut = fs.create(file, progress)
      val fileOutWithCodec = new DataOutputStream(codec.createOutputStream(fileOut))
      getRecordWriter(fileOutWithCodec)
    }
  }
}
