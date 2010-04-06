package com.twitter.rowz.integration

import com.twitter.gizzard.thrift.ShardManagerService
import org.specs.Specification
import com.twitter.gizzard.scheduler.Priority
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
    val shardInfo = new ShardInfo("com.twitter.rowz.SqlShard", "shard_a", "localhost")
    val queryEvaluator = queryEvaluatorFactory("localhost", null, config("rowz.db.username"), config("rowz.db.password"))

    doBefore {
      state.nameServer.rebuildSchema()
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config("rowz.db.name"))

      val shardId = state.nameServer.createShard(shardInfo)
      state.nameServer.setForwarding(new Forwarding(0, Math.MIN_LONG, shardId))
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
  }
}
