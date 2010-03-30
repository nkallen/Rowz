package com.twitter.rowz

import com.twitter.gizzard.shards
import com.twitter.gizzard.shards.ReadWriteShard
import com.twitter.xrayspecs.Time


class ReadWriteShardAdapter(shard: ReadWriteShard[Shard])
  extends shards.ReadWriteShardAdapter(shard) with Shard {

  def create(info: RowInfo, at: Time) = shard.writeOperation(_.create(info, at))
  def destroy(id: Long, at: Time) = shard.writeOperation(_.destroy(id, at))
  def read(id: Long) = shard.readOperation(_.read(id))
}