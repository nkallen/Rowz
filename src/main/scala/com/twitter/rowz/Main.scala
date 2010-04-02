package com.twitter.rowz

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import com.twitter.gizzard.nameserver.{NameServer, Copier}
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import com.twitter.gizzard.thrift.{TSelectorServer, JobManager, JobManagerService, ShardManager, ShardManagerService}
import com.facebook.thrift.server.{TServer, TThreadPoolServer}
import com.facebook.thrift.transport.{TServerSocket, TTransportFactory}
import com.twitter.ostrich.{W3CStats, Stats}
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.proxy.LoggingProxy


object Main {
  var rowzService: RowzService = null
  var nameServer: NameServer[Shard] = null
  var scheduler: PrioritizingJobScheduler = null
  var copier: Copier[Shard] = null

  var rowzServer: TSelectorServer = null
  var jobServer: TSelectorServer = null
  var shardServer: TSelectorServer = null

  val config = Configgy.config
  val w3c = new W3CStats(Logger.get("w3c"), Array(
    "action-timing",
    "db-timing",
    "connection-pool-release-timing",
    "connection-pool-reserve-timing",
    "kestrel-put-timing",
    "db-select-count",
    "db-execute-count",
    "operation",
    "arguments"
  ))

  def main(args: Array[String]) {
    val state = Rowz(config, w3c)
    rowzService = state._1
    nameServer = state._2
    scheduler = state._3
    copier = state._4

    startThrift()
  }

  def startThrift() {
    val timeout = config("timeout").toInt.milliseconds
    val executor = TSelectorServer.makeThreadPoolExecutor(config)
    val processor = new rowz.thrift.Rowz.Processor(LoggingProxy[rowz.thrift.Rowz.Iface](Stats, w3c, "Rowz", rowzService))
    rowzServer = TSelectorServer("rowz", config("port").toInt, processor, executor, timeout)

    val jobService = new JobManagerService(scheduler)
    val jobProcessor = new JobManager.Processor(LoggingProxy[JobManager.Iface](Stats, Main.w3c, "RowzJobs", jobService))
    jobServer = TSelectorServer("rowz-jobs", config("rowz.job_server_port").toInt, jobProcessor, executor, timeout)

    val shardService = new ShardManagerService(nameServer, copier, scheduler(Priority.Low.id))
    val shardProcessor = new ShardManager.Processor(ExceptionWrappingProxy(LoggingProxy[ShardManager.Iface](Stats, Main.w3c, "RowzShards", shardService)))
    shardServer = TSelectorServer("rowz-shards", config("rowz.shard_server_port").toInt, shardProcessor, executor, timeout)

    rowzServer.serve()
    jobServer.serve()
    shardServer.serve()
  }

  def shutdown() {
    rowzServer.stop()
    jobServer.stop()
    scheduler.shutdown()

    System.exit(0)
  }
}
