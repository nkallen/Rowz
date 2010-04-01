package com.twitter.rowz

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, Busy}

object SqlShard extends Specification with JMocker with ClassMocker {
  "SqlShard" should {
    import Database._
    Time.freeze()

    val shardFactory = new SqlShardFactory(queryEvaluatorFactory, config)
    val shardInfo = new ShardInfo("com.twitter.service.flock.edges.SqlShard",
      "table_001", "localhost", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal, 1)
    val sqlShard = shardFactory.instantiate(shardInfo, 1, List[Shard]())
    val queryEvaluator = queryEvaluatorFactory(shardInfo.hostname, null, config("rowz.db.username"), config("rowz.db.password"))
    val row = new Row(1, "a row", Time.now)

    doBefore {
      queryEvaluator.execute("DROP DATABASE IF EXISTS " + config("rowz.db.name"))
      shardFactory.materialize(shardInfo)
    }

    "create & read" in {
      sqlShard.create(row.id, row.name, row.createdAt)
      sqlShard.read(row.id) mustEqual Some(row)
    }

    "create, destroy then read" in {
      sqlShard.create(row.id, row.name, row.createdAt)
      sqlShard.destroy(row, row.createdAt + 1.second)
      sqlShard.read(row.id) mustEqual None
    }

    "idempotent" in {
      "read a nonexistent row" in {
        sqlShard.read(row.id) mustEqual None
      }

      "destroy, create, then read" in {
        sqlShard.destroy(row, row.createdAt)
        sqlShard.create(row.id, row.name, row.createdAt - 1.second)
        sqlShard.read(row.id) mustEqual None
      }
    }
  }
}
