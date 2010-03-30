package com.twitter.rowz

import net.lag.configgy.Configgy
import net.lag.logging.Logger
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.JobScheduler
import com.twitter.gizzard.thrift.{TSelectorServer, JobManagerService}
import com.facebook.thrift.server.{TServer, TThreadPoolServer}
import com.facebook.thrift.transport.{TServerSocket, TTransportFactory}
import com.twitter.ostrich.W3CStats


object Main {
  var rowzService: RowzService = null
  var nameServer: NameServer[Shard] = null
  var scheduler: JobScheduler = null
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
    val state = Rowz(config)
    rowzService = state._0
    nameServer = state._1
    scheduler = state._2

    startThrift()
  }

  def startThrift() {
    val executor = TSelectorServer.makeThreadPoolExecutor(config)
    val processor = new rowz.thrift.Rowz.Processor(ExceptionWrappingProxy[rowz.thrift.Rowz.Iface](LoggingProxy[rowz.thrift.Rowz.Iface](Stats, w3c, "Rowz", rowzService)))
    rowzServer = TSelectorServer("rowz", config("port").toInt, processor, executor, config("timeout").toInt.milliseconds)

    val jobService = new JobManagerService(scheduler)
    val jobProcessor = new JobManager.Processor(ExceptionWrappingProxy[JobManager.Iface](LoggingProxy[JobManager.Iface](Stats, Main.w3c, "RowzJobs", jobService)))
    jobServer = TSelectorServer("rowz-jobs", config("edges.job_server_port").toInt, jobProcessor, executor, config("timeout").toInt.milliseconds)

    val shardService = new ShardManagerService(nameServer)
    val shardProcessor = new ShardManager.Processor(ExceptionWrappingProxy[ShardManager.Iface](LoggingProxy[ShardManager.Iface](Stats, Main.w3c, "RowzShards", shardService)))
    shardServer = TSelectorServer("edges-shards", config("edges.shard_server_port").toInt, edgesShardProcessor, edgesExecutor, edgesTimeout)

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
