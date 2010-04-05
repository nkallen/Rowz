package com.twitter.rowz.integration

import com.twitter.gizzard.thrift.ShardManagerService
import org.specs.Specification
import com.twitter.gizzard.scheduler.Priority
import com.twitter.xrayspecs.{Time, Eventually}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import com.twitter.gizzard.shards.ShardInfo
import com.twitter.gizzard.nameserver.Forwarding
import com.twitter.gizzard.thrift.conversions.ShardInfo._


object RowzSpec extends Specification with Eventually {
  "Rowz" should {
    import Database._
    Time.freeze()
    val state = Rowz(config, w3c, databaseFactory)
    val rowzService = state.rowzService
    val shardManagerService = new ShardManagerService(state.nameServer, state.copyFactory, state.prioritizingScheduler(Priority.Medium.id))

    doBefore {
      state.nameServer.rebuildSchema()
      val partitions = 1
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
        state.nameServer.setForwarding(new Forwarding(0, Math.MIN_LONG + (Math.MAX_LONG - Math.MIN_LONG) / partitions * i, replicatingShardId))
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
      val id = rowzService.create("row", Time.now.inSeconds)
      rowzService.read(id) must eventually(not(throwA[Exception]))

      val sourceShardInfo = new ShardInfo("com.twitter.rowz.SqlShard", "shard_a" + 0, "localhost")
      val destinationShardInfo = new ShardInfo("com.twitter.rowz.SqlShard", "shard_c" + 0, "localhost")
      val migration = shardManagerService.setup_migration(sourceShardInfo.toThrift, destinationShardInfo.toThrift)
      shardManagerService.reload_forwardings()
      shardManagerService.migrate_shard(migration)
    }
  }
}
