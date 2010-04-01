package com.twitter.rowz

import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import net.lag.configgy.ConfigMap
import com.twitter.gizzard.shards
import com.twitter.querulous.query.SqlQueryTimeoutException
import java.sql.SQLException
import com.twitter.gizzard.proxy.SqlExceptionWrappingProxy
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


class SqlShardFactory(queryEvaluatorFactory: QueryEvaluatorFactory, config: ConfigMap)
  extends shards.ShardFactory[Shard] {

  val TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  id                    BIGINT                   NOT NULL,
  name                  VARCHAR(255)             NOT NULL,
  created_at            INT UNSIGNED             NOT NULL,
  updated_at            INT UNSIGNED             NOT NULL,
  state                 TINYINT                  NOT NULL,

  PRIMARY KEY (id)
) TYPE=INNODB"""

  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) = {
    val queryEvaluator = queryEvaluatorFactory(List(shardInfo.hostname), config("rowz.db.name"), config("rowz.db.username"), config("rowz.db.password"))
    SqlExceptionWrappingProxy[Shard](new SqlShard(queryEvaluator, shardInfo, weight, children))
  }

  def materialize(shardInfo: shards.ShardInfo) = {
    try {
      val queryEvaluator = queryEvaluatorFactory(
        List(shardInfo.hostname),
        config("rowz.db.name"),
        config("rowz.db.username"),
        config("rowz.db.password"))
      queryEvaluatorFactory(shardInfo.hostname, null, config("rowz.db.username"), config("rowz.db.password")).execute("CREATE DATABASE IF NOT EXISTS " + config("rowz.db.name"))
      queryEvaluator.execute(TABLE_DDL.format(shardInfo.tablePrefix + "_rowz"))
    } catch {
      case e: SQLException => throw new shards.ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new shards.ShardTimeoutException
    }
  }
}


class SqlShard(private val queryEvaluator: QueryEvaluator, val shardInfo: shards.ShardInfo,
               val weight: Int, val children: Seq[Shard]) extends Shard {

  private val table = shardInfo.tablePrefix + "_rowz"

  def create(id: Long, name: String, at: Time) = {
    queryEvaluator.execute("INSERT INTO " + table + " (id, name, created_at, updated_at, state) VALUES (?, ?, ?, ?, ?)",
      id, name, at.inSeconds, at.inSeconds, State.Normal.id)
  }

  def destroy(row: Row, at: Time) = ()

  def read(id: Long) = {
    queryEvaluator.selectOne("SELECT * FROM " + table + " WHERE id = ?", id) { row =>
      new Row(row.getLong("id"), row.getString("name"), Time(row.getLong("created_at").seconds))
    }
  }
}