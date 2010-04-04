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
      val shard1 = new ShardInfo("com.twitter.rowz.SqlShard", "shard_1", "localhost")
      val shardId = state.nameServer.createShard(shard1)
      state.nameServer.setForwarding(new Forwarding(0, 0, shardId))
      // state.nameServer.createShard(shard2)
      state.start()
    }

    "row create & read" in {
      println("1")
      val id = rowzService.create("row", Time.now.inSeconds)
      Thread.sleep(5.seconds.inMillis)
      println("2")
      rowzService.read(id)
      println("3")
      val row = rowzService.read(id)
      println("4")
      row.name mustEqual "row"
      println("5")
      rowzService.destroy(row, 1.second.fromNow.inSeconds)
      println("6")
      rowzService.read(id) must eventually(throwA[thrift.RowzException])
    }
  }
}
