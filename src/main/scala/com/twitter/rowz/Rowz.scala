package com.twitter.rowz

import net.lag.configgy.Config
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.{Logger, ThrottledLogger}
import com.twitter.gizzard.Future
import com.twitter.gizzard.scheduler.{JobScheduler, PrioritizingJobScheduler}
import com.twitter.gizzard.shards._
import com.twitter.gizzard.nameserver.{NameServer, ShardRepository, SqlNameServerStore}
import com.twitter.gizzard.jobs.{PolymorphicJobParser, BoundJobParser}
import scala.collection.mutable
import com.twitter.ostrich.W3CStats


object Rowz {
  def apply(config: Config, w3c: W3CStats) = {
    val log = Logger.get
    val databaseFactory = new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(
      config("rowz.db.connection_pool.size_min").toInt,
      config("rowz.db.connection_pool.size_max").toInt,
      config("rowz.db.connection_pool.test_idle_msec").toLong.millis,
      config("rowz.db.connection_pool.max_wait").toLong.millis,
      config("rowz.db.connection_pool.test_on_borrow").toBoolean,
      config("rowz.db.connection_pool.min_evictable_idle_msec").toLong.millis))

    val queryEvaluatorFactory    = new StandardQueryEvaluatorFactory(databaseFactory, new SqlQueryFactory)

    val throttledLogger         = new ThrottledLogger[String](Logger(), config("throttled_log.period_msec").toInt, config("throttled_log.rate").toInt)
    val future                  = new Future("ReplicatingFuture", config.configMap("replication.future"))

    val shardRepository         = new ShardRepository[Shard]
    shardRepository             += ("com.twitter.rowz.SqlShard"                   -> new SqlShardFactory(queryEvaluatorFactory, config))
    shardRepository             += ("com.twitter.gizzard.shards.ReadOnlyShard"    -> new ReadOnlyShardFactory(new ReadWriteShardAdapter(_)))
/*    shardRepository             += ("com.twitter.gizzard.shards.BlockedShard"     -> new BlockedShardFactory)
    shardRepository             += ("com.twitter.gizzard.shards.WriteOnlyShard"   -> new WriteOnlyShardFactory)
    shardRepository             += ("com.twitter.gizzard.shards.ReplicatingShard" -> new ReplicatingShardFactory(throttledLogger, future))
*/

    val nameServerStores = config.getList("rowz.nameserver.databases").map { hostname =>
      new SqlNameServerStore(
        queryEvaluatorFactory(
          hostname,
          config("rowz.nameserver.name"),
          config("rowz.nameserver.username"),
          config("rowz.nameserver.password")))
    }
/*    val replicatingNameServerStore = new ReplicatingNameServerStore(nameServerStores, log, future)*/
    val nameServer                 = new NameServer(nameServerStores.first, shardRepository, Hash)
    val forwardingManager          = new ForwardingManager(nameServer)

/*    val copyJobParser           = new BoundJobParser((nameServer, scheduler))*/
    val rowzJobParser           = new BoundJobParser(forwardingManager)

    val polymorphicJobParser    = new PolymorphicJobParser
/*    polymorphicJobParser        += ("rowz\\.jobs\\.(Copy|Migrate)".r, copyJobParser)*/
    polymorphicJobParser        += ("rowz\\.jobs\\.(Create|Destroy)".r, rowzJobParser)
    val schedulerMap = new mutable.HashMap[Int, JobScheduler]
    List((Priority.High, "high"), (Priority.Low, "low")).foreach { case (priority, configName) =>
      val queueConfig = config.configMap("edges.queue")
      val scheduler = JobScheduler(configName, queueConfig, polymorphicJobParser, w3c)
      schedulerMap(priority.id) = scheduler
    }
    val prioritizingScheduler = new PrioritizingJobScheduler(schedulerMap)
    val copyManager = new CopyManager(prioritizingScheduler(Priority.Low.id))

    val rowzService                = new RowzService(forwardingManager, prioritizingScheduler)

    nameServer.reload()
    prioritizingScheduler.start()

    (rowzService, nameServer, prioritizingScheduler, copyManager)
  }
}