package com.twitter.rowz.jobs

import com.twitter.gizzard
import Shard.Cursor
import com.twitter.xrayspecs.TimeConversions._


object Copy {
  val COUNT = 500
}

object CopyFactory extends gizzard.jobs.CopyFactory[Shard] {
  def apply(sourceShardId: Int, destinationShardId: Int) = new Copy(sourceShardId, destinationShardId, Shard.CursorStart)
}

class Copy(sourceShardId: Int, destinationShardId: Int, cursor: Cursor, count: Int) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def this(sourceShardId: Int, destinationShardId: Int, cursor: Cursor) = this(sourceShardId, destinationShardId, cursor, Copy.COUNT)
  def this(attributes: Map[String, AnyVal]) = {
    this(
      attributes("source_shard_id").toInt,
      attributes("destination_shard_id").toInt,
      attributes("cursor").toInt,
      attributes("count").toInt)
  }

  def serialize = Map("cursor" -> cursor)

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, nextCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.write(items)
    nextCursor.map(new Copy(sourceShardId, destinationShardId, _))
  }
}
