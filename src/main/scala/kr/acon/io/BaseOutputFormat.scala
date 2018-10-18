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

package kr.acon.io

import java.io.DataOutputStream

import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, RecordWriter}
import org.apache.hadoop.util.{Progressable, ReflectionUtils}

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
