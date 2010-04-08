package com.twitter.rowz

import com.twitter.gizzard.shards
import com.twitter.gizzard.shards.ReadWriteShard
import com.twitter.xrayspecs.Time
import Shard.Cursor


class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def create(id: Long, name: String, at: Time) = shard.writeOperation(_.create(id, name, at))
  def destroy(row: Row, at: Time)              = shard.writeOperation(_.destroy(row, at))
  def write(rows: Seq[Row])                    = shard.writeOperation(_.write(rows))

  def read(id: Long)                           = shard.readOperation(_.read(id))
  def selectAll(cursor: Cursor, count: Int)    = shard.readOperation(_.selectAll(cursor, count))
}
