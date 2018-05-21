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

package kr.acon.lib.io

import java.io.DataOutputStream

import kr.acon.lib.io.recordwriter.CSR4RecordWriter
import kr.acon.lib.io.recordwriter.CSR6RecordWriter
import kr.acon.lib.io.recordwriter.CSR8RecordWriter

class CSR4OutputFormat
    extends BaseOutputFormat {
  @inline final override def getRecordWriter(out: DataOutputStream) = new CSR4RecordWriter(out)
}

class CSR6OutputFormat
    extends BaseOutputFormat {
  @inline final override def getRecordWriter(out: DataOutputStream) = new CSR6RecordWriter(out)
}

class CSR8OutputFormat
    extends BaseOutputFormat {
  @inline final override def getRecordWriter(out: DataOutputStream) = new CSR8RecordWriter(out)
}
