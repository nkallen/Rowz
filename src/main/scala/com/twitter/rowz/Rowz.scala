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
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.nameserver.{NameServer, ShardRepository, LoadBalancer}
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
    shardRepository             += ("com.twitter.gizzard.shards.BlockedShard"     -> new BlockedShardFactory(new ReadWriteShardAdapter(_)))
    shardRepository             += ("com.twitter.gizzard.shards.WriteOnlyShard"   -> new WriteOnlyShardFactory(new ReadWriteShardAdapter(_)))
    shardRepository             += ("com.twitter.gizzard.shards.ReplicatingShard" -> new ReplicatingShardFactory(new ReadWriteShardAdapter(_), throttledLogger, { (x, y) => }, future))


    val nameServerShards = config.getList("rowz.nameserver.databases").map { hostname =>
      new nameserver.SqlShard(
        queryEvaluatorFactory(
          hostname,
          config("rowz.nameserver.name"),
          config("rowz.nameserver.username"),
          config("rowz.nameserver.password")))
    }

    val replicatingNameServerShard = new nameserver.ReadWriteShardAdapter(new ReplicatingShard(
      new ShardInfo("com.twitter.gizzard.shards.ReplicatingShard", "", ""),
      1, nameServerShards, new LoadBalancer(nameServerShards), throttledLogger, future, { (x, y) => }))
    val nameServer                 = new NameServer(replicatingNameServerShard, shardRepository, Hash)
    val forwardingManager          = new ForwardingManager(nameServer)


    val polymorphicJobParser    = new PolymorphicJobParser
    val schedulerMap = new mutable.HashMap[Int, JobScheduler]
    List((Priority.High, "high"), (Priority.Low, "low")).foreach { case (priority, configName) =>
      val queueConfig = config.configMap("edges.queue")
      val scheduler = JobScheduler(configName, queueConfig, polymorphicJobParser, w3c)
      schedulerMap(priority.id) = scheduler
    }
    val prioritizingScheduler = new PrioritizingJobScheduler(schedulerMap)

    val copyJobParser           = new BoundJobParser((nameServer, prioritizingScheduler(Priority.Low.id)))
    val rowzJobParser           = new BoundJobParser(forwardingManager)
    polymorphicJobParser        += ("rowz\\.jobs\\.(Copy|Migrate)".r, copyJobParser)
    polymorphicJobParser        += ("rowz\\.jobs\\.(Create|Destroy)".r, rowzJobParser)

    val rowzService             = new RowzService(forwardingManager, prioritizingScheduler, new IdGenerator(config("host.id").toInt))

    nameServer.reload()
    prioritizingScheduler.start()

    (rowzService, nameServer, prioritizingScheduler, new jobs.Copy(_, _, Shard.CursorStart))
  }
}