package com.twitter.rowz.jobs

import com.twitter.gizzard
import Shard.Cursor


object Copy {
  val COUNT = 500
}

object CopyFactory extends gizzard.jobs.CopyFactory[Shard] {
  def apply(sourceShardId: Int, destinationShardId: Int) = new Copy(sourceShardId, destinationShardId, Shard.CursorStart)
}

class Copy(sourceShardId: Int, destinationShardId: Int, cursor: Cursor) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, Copy.COUNT) {
  def serialize = Map("cursor" -> cursor)
  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, nextCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.write(items)
    nextCursor.map(new Copy(sourceShardId, destinationShardId, _))
  }
}
