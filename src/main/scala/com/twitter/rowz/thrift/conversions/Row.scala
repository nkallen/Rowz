package com.twitter.rowz.thrift.conversions

import RowInfo._


object Row {
  class RichShardingRow(row: rowz.Row) {
    def toThrift = new thrift.Row(row.id, row.info.toThrift)
  }
  implicit def shardingRowToRichShardingRow(row: rowz.Row) = new RichShardingRow(row)

  class RichThriftRow(row: thrift.Row) {
    def fromThrift = new rowz.Row(row.id, row.info.fromThrift)
  }
  implicit def thriftRowToRichThriftRow(row: thrift.Row) = new RichThriftRow(row)
}
