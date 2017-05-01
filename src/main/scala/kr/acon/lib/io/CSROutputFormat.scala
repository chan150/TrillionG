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
