package com.twitter.rowz.jobs

import com.twitter.gizzard
import Shard.Cursor


object Copy {
  val COUNT = 500
}

case class Copy(sourceShardId: Int, destinationShardId: Int, cursor: Cursor) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, Copy.COUNT) {
  protected def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, nextCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.write(items)
    nextCursor.map(new Copy(sourceShardId, destinationShardId, _))
  }
}
