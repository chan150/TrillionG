package kr.acon.lib.io

import java.io.DataOutputStream

import kr.acon.lib.io.recordwriter.TSVRecordWriter

class TSVOutputFormat
    extends BaseOutputFormat {
  @inline final override def getRecordWriter(out: DataOutputStream) = new TSVRecordWriter(out)
}