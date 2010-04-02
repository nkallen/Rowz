package com.twitter.rowz.thrift.conversions

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


object Row {
  class RichShardingRow(row: rowz.Row) {
    def toThrift = new thrift.Row(row.id, row.name, row.createdAt.inSeconds, row.updatedAt.inSeconds, row.state.id)
  }
  implicit def shardingRowToRichShardingRow(row: rowz.Row) = new RichShardingRow(row)

  class RichThriftRow(row: thrift.Row) {
    def fromThrift = new rowz.Row(row.id, row.name, Time(row.created_at.seconds), Time(row.updated_at.seconds), State(row.state))
  }
  implicit def thriftRowToRichThriftRow(row: thrift.Row) = new RichThriftRow(row)
}
