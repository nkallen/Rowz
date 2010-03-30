package com.twitter.rowz

import net.lag.configgy.Config
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.{Logger, ThrottledLogger}
import com.twitter.gizzard.Future
import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.gizzard.nameserver.{NameServer, ShardRepository}
import com.twitter.gizzard.jobs.PolymorphicJobParser


object Rowz {
  def apply(config: Config) = {
    val databaseFactory = new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(
      config("rowz.db.connection_pool.size_min").toInt,
      config("rowz.db.connection_pool.size_max").toInt,
      config("rowz.db.connection_pool.test_idle_msec").toLong.millis,
      config("rowz.db.connection_pool.max_wait").toLong.millis,
      config("rowz.db.connection_pool.test_on_borrow").toBoolean,
      config("rowz.db.connection_pool.min_evictable_idle_msec").toLong.millis))

    val queryEvaluatorFactory = new StandardQueryEvaluatorFactory(databaseFactory, new SqlQueryFactory)
    val nameServerQueryEvaluator = queryEvaluatorFactory(
      config("nameserver.hostname"),
      config("nameserver.database"),
      config("nameserver.username"),
      config("nameserver.password"))

    val throttledLogger         = new ThrottledLogger[String](Logger(), config("throttled_log.period_msec").toInt, config("throttled_log.rate").toInt)
    val future                  = new Future("ReplicatingFuture", config.configMap("replication.future"))

    val shardRepository    = new ShardRepository[Shard]
    shardRepository        += ("com.twitter.rowz.SqlShard"            -> new SqlShardFactory(queryEvaluatorFactory, config))
    shardRepository        += ("com.twitter.gizzard.ReadOnlyShard"    -> new gizzard.ReadOnlyShardFactory)
    shardRepository        += ("com.twitter.gizzard.BlockedShard"     -> new gizzard.BlockedShardFactory)
    shardRepository        += ("com.twitter.gizzard.WriteOnlyShard"   -> new gizzard.WriteOnlyShardFactory)
    shardRepository        += ("com.twitter.gizzard.ReplicatingShard" -> new gizzard.ReplicatingShardFactory(throttledLogger, replicatingFuture))

    val polymorphicJobParser    = new PolymorphicJobParser
    val scheduler               = JobScheduler("jobs", queueConfig, jobParser, Main.w3c)

    val queryExecutorFuture     = new Future("QueryExecutorFuture", config.configMap("groups.query_executor_future"))

    val forwardingManager  = new ForwardingManager(Hash, nameServerQueryEvaluator)
    val copyManager        = new CopyManager(scheduler)
    val nameServer         = new NameServer[Shard](nameServerQueryEvaluator, shardRepository, forwardingManager, copyManager)
    val rowzService        = new RowzService(nameServer, forwardingManager, scheduler)

    nameServer.reload()
    scheduler.start()

    (rowzService, nameServer, scheduler)
  }
}