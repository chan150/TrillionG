package kr.acon.lib.io

import java.io.DataOutputStream

import kr.acon.lib.io.recordwriter.ADJ6RecordWriter
import kr.acon.lib.io.recordwriter.ADJ8RecordWriter
import kr.acon.lib.io.recordwriter.ADJ4RecordWriter

class ADJ4OutputFormat
    extends BaseOutputFormat {
  @inline final override def getRecordWriter(out: DataOutputStream) = new ADJ4RecordWriter(out)
}

class ADJ6OutputFormat
    extends BaseOutputFormat {
  @inline final override def getRecordWriter(out: DataOutputStream) = new ADJ6RecordWriter(out)
}

class ADJ8OutputFormat
    extends BaseOutputFormat {
  @inline final override def getRecordWriter(out: DataOutputStream) = new ADJ8RecordWriter(out)
}
