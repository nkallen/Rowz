package com.twitter.rowz.jobs

import com.twitter.gizzard
import Shard.Cursor
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.xrayspecs.TimeConversions._


object CopyFactory extends gizzard.jobs.CopyFactory[Shard] {
  val COUNT = 500

  def apply(sourceShardId: Int, destinationShardId: Int) = new Copy(sourceShardId, destinationShardId, Shard.CursorStart, COUNT)
}

object CopyParser extends gizzard.jobs.CopyParser[Shard] {
  def apply(attributes: Map[String, Any]) = {
    val casted = attributes.asInstanceOf[Map[String, AnyVal]]
    new Copy(
      casted("source_shard_id").toInt,
      casted("destination_shard_id").toInt,
      casted("cursor").toLong,
      casted("count").toInt)
  }
}

class Copy(sourceShardId: Int, destinationShardId: Int, cursor: Cursor, count: Int) extends gizzard.jobs.Copy[Shard](sourceShardId, destinationShardId, count) {
  def serialize = Map("cursor" -> cursor)

  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = {
    val (items, nextCursor) = sourceShard.selectAll(cursor, count)
    destinationShard.write(items)
    nextCursor.map(new Copy(sourceShardId, destinationShardId, _, count))
  }
}
