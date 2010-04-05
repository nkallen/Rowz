package com.twitter.rowz.integration

import org.specs.Specification
import com.twitter.xrayspecs.{Time, Eventually}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.nameserver.Forwarding


object RowzSpec extends Specification with Eventually {
  "Rowz" should {
    import Database._
    Time.freeze()
    val state = Rowz(config, w3c, databaseFactory)
    val rowzService = state.rowzService

    doBefore {
      state.nameServer.rebuildSchema()
      val partitions = 2
      (0 until partitions) foreach { i =>
        val shardInfoA = new ShardInfo("com.twitter.rowz.SqlShard", "shard_a" + i, "localhost")
        val shardInfoB = new ShardInfo("com.twitter.rowz.SqlShard", "shard_b" + i, "localhost")
        val replicatingShardInfo = new ShardInfo("com.twitter.gizzard.shards.ReplicatingShard", "replicating_" + i, "localhost")
        val shardIdA = state.nameServer.createShard(shardInfoA)
        val shardIdB = state.nameServer.createShard(shardInfoB)
        val replicatingShardId = state.nameServer.createShard(replicatingShardInfo)

        val weight = 1
        state.nameServer.addChildShard(replicatingShardId, shardIdA, weight)
        state.nameServer.addChildShard(replicatingShardId, shardIdB, weight)
        state.nameServer.setForwarding(new Forwarding(0, Math.MIN_LONG, replicatingShardId))
      }
      state.start()
    }

    "row create & read" in {
      val id = rowzService.create("row", Time.now.inSeconds)
      rowzService.read(id) must eventually(not(throwA[Exception]))
      val row = rowzService.read(id)
      row.name mustEqual "row"
      rowzService.destroy(row, 1.second.fromNow.inSeconds)
      rowzService.read(id) must eventually(throwA[Exception])
    }

    "shard migration" in {
      
    }
  }
}
