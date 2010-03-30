package com.twitter.rowz.thrift.conversions

import State._


object RowInfo {
  class RichShardingRowInfo(rowInfo: rowz.RowInfo) {
    def toThrift = new thrift.RowInfo(rowInfo.name, rowInfo.state.toThrift)
  }
  implicit def shardingRowInfoToRichShardingRowInfo(rowInfo: rowz.RowInfo) = new RichShardingRowInfo(rowInfo)

  class RichThriftRowInfo(rowInfo: thrift.RowInfo) {
    def fromThrift = new rowz.RowInfo(rowInfo.name, rowInfo.state_id.fromThrift)
  }
  implicit def thriftRowInfoToRichThriftRowInfo(rowInfo: thrift.RowInfo) = new RichThriftRowInfo(rowInfo)
}
