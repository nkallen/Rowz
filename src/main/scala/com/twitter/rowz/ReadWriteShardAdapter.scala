package com.twitter.rowz

import com.twitter.gizzard.shards
import com.twitter.gizzard.shards.ReadWriteShard
import com.twitter.xrayspecs.Time


class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def create(id: Long, name: String, at: Time) = shard.writeOperation(_.create(id, name, at))
  def destroy(row: Row, at: Time)              = shard.writeOperation(_.destroy(row, at))

  def read(id: Long)                           = shard.readOperation(_.read(id))
}