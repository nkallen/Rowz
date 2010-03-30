package com.twitter.rowz

import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import net.lag.configgy.ConfigMap
import com.twitter.gizzard.shards
import com.twitter.querulous.query.SqlQueryTimeoutException
import java.sql.SQLException
import com.twitter.gizzard.proxy.SqlExceptionWrappingProxy
import com.twitter.xrayspecs.Time


class SqlShardFactory(queryEvaluatorFactory: QueryEvaluatorFactory, config: ConfigMap)
  extends shards.ShardFactory[Shard] {

  val TABLE_DDL = """
CREATE TABLE IF NOT EXISTS %s (
  source_id             %s                       NOT NULL,
  position              BIGINT                   NOT NULL,
  updated_at            INT UNSIGNED             NOT NULL,
  destination_id        %s                       NOT NULL,
  count                 TINYINT UNSIGNED         NOT NULL,
  state                 TINYINT                  NOT NULL,

  PRIMARY KEY (source_id, state, position),

  UNIQUE unique_source_id_destination_id (source_id, destination_id)
) TYPE=INNODB"""

  def instantiate(shardInfo: shards.ShardInfo, weight: Int, children: Seq[Shard]) = {
    val queryEvaluator = queryEvaluatorFactory(List(shardInfo.hostname), config("rowz.db.name"), config("rowz.db.username"), config("rowz.db.password"))
    SqlExceptionWrappingProxy[Shard](new SqlShard(queryEvaluator, shardInfo, weight, children, config))
  }

  def materialize(shardInfo: shards.ShardInfo) = {
    try {
      val queryEvaluator = queryEvaluatorFactory(
        List(shardInfo.hostname),
        config("rowz.db.name"),
        config("rowz.db.username"),
        config("rowz.db.password"))
      queryEvaluatorFactory(shardInfo.hostname, null, config("rowz.db.username"), config("rowz.db.password")).execute("CREATE DATABASE IF NOT EXISTS " + config("rowz.db.name"))
      queryEvaluator.execute(TABLE_DDL.format(shardInfo.tablePrefix + "_rowz", shardInfo.sourceType, shardInfo.destinationType))
    } catch {
      case e: SQLException => throw new shards.ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new shards.ShardTimeoutException
    }
  }
}


class SqlShard(private val queryEvaluator: QueryEvaluator, val shardInfo: shards.ShardInfo,
               val weight: Int, val children: Seq[Shard], config: ConfigMap) extends Shard {

  def create(info: RowInfo, at: Time) = ()
  def destroy(id: Long, at: Time) = ()
  def read(id: Long) = null
}