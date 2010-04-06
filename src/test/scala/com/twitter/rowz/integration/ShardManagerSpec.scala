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


object ShardManagerSpec extends Specification with Eventually {
  "ShardManager" should {
    import Database._
    Time.freeze()
    val state = Rowz(config, w3c, databaseFactory)
    val rowzService = state.rowzService
    val shardManagerService = new ShardManagerService(state.nameServer, state.copyFactory, state.prioritizingScheduler(Priority.Medium.id))
    val shardInfoA = new ShardInfo("com.twitter.rowz.SqlShard", "shard_a", "localhost")
    val shardInfoB = new ShardInfo("com.twitter.rowz.SqlShard", "shard_b", "localhost")
    val replicatingShardInfo = new ShardInfo("com.twitter.gizzard.shards.ReplicatingShard", "replicating", "localhost")
    val queryEvaluator = queryEvaluatorFactory("localhost", null, config("rowz.db.username"), config("rowz.db.password"))
    var replicatingShardId = 0
    var shardIdA = 0
    var shardIdB = 0

    doBefore {
      state.nameServer.rebuildSchema()
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config("rowz.db.name"))

      shardIdA = state.nameServer.createShard(shardInfoA)
      shardIdB = state.nameServer.createShard(shardInfoB)
      replicatingShardId = state.nameServer.createShard(replicatingShardInfo)

      val weight = 1
      state.nameServer.addChildShard(replicatingShardId, shardIdA, weight)
      state.nameServer.addChildShard(replicatingShardId, shardIdB, weight)
      state.nameServer.setForwarding(new Forwarding(0, Math.MIN_LONG, replicatingShardId))
      state.start()
    }

    "replication" in {
      val id = rowzService.create("row", Time.now.inSeconds)
      rowzService.read(id) must eventually(not(throwA[Exception]))

      shardManagerService.replace_forwarding(replicatingShardId, shardIdA)
      shardManagerService.reload_forwardings()
      rowzService.read(id) must eventually(not(throwA[Exception]))

      shardManagerService.replace_forwarding(replicatingShardId, shardIdB)
      shardManagerService.reload_forwardings()
      rowzService.read(id) must eventually(not(throwA[Exception]))
    }

    "shard migration" in {
      val id = rowzService.create("row", Time.now.inSeconds)
      rowzService.read(id) must eventually(not(throwA[Exception]))

      val sourceShardInfo = shardInfoA
      val destinationShardInfo = new ShardInfo("com.twitter.rowz.SqlShard", "shard_c" + 0, "localhost")
      val migration = shardManagerService.setup_migration(sourceShardInfo.toThrift, destinationShardInfo.toThrift)
      shardManagerService.reload_forwardings()
      shardManagerService.migrate_shard(migration)
      shardManagerService.replace_forwarding(replicatingShardId, migration.destination_shard_id)
      shardManagerService.reload_forwardings()
      rowzService.read(id) must eventually(not(throwA[Exception]))
    }
  }
}
