package com.twitter.rowz.unit

import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, Busy}

object SqlShard extends ConfiguredSpecification with JMocker with ClassMocker {
  "SqlShard" should {
    import Database._
    Time.freeze()

    val shardFactory = new SqlShardFactory(queryEvaluatorFactory, config)
    val shardInfo = new ShardInfo("com.twitter.service.flock.edges.SqlShard",
      "table_001", "localhost", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal, 1)
    val sqlShard = shardFactory.instantiate(shardInfo, 1, List[Shard]())
    val row = new Row(1, "a row", Time.now, Time.now, State.Normal)
    val row2 = new Row(2, "another row", Time.now, Time.now, State.Normal)
    val queryEvaluator = queryEvaluatorFactory("localhost", null, config("rowz.db.username"), config("rowz.db.password"))

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

    "selectAll" in {
      doBefore {
        sqlShard.create(row.id, row.name, row.createdAt)
        sqlShard.create(row2.id, row2.name, row2.createdAt)
      }

      "start cursor" in {
        val (rows, nextCursor) = sqlShard.selectAll(-1, 1)
        rows.toList mustEqual List(row)
        nextCursor mustEqual Some(row.id)
      }

      "multiple items" in {
        val (rows, nextCursor) = sqlShard.selectAll(-1, 2)
        rows.toList mustEqual List(row, row2)
        nextCursor mustEqual None
      }

      "non-start cursor" in {
        val (rows, nextCursor) = sqlShard.selectAll(row.id, 1)
        rows.toList mustEqual List(row2)
        nextCursor mustEqual None
      }
    }
  }
}
