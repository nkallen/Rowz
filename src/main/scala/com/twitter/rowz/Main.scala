package com.twitter.rowz

import com.twitter.gizzard.thrift.{JobManagerService, ShardManagerService}
import net.lag.configgy.{Configgy, RuntimeEnvironment, ConfigMap}
import net.lag.logging.Logger
import com.twitter.gizzard.jobs.CopyFactory
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.{PrioritizingJobScheduler, Priority}
import com.twitter.gizzard.thrift.{TSelectorServer, JobManager, ShardManager}
import com.facebook.thrift.server.{TServer, TThreadPoolServer}
import com.facebook.thrift.transport.{TServerSocket, TTransportFactory}
import com.twitter.ostrich.{W3CStats, Stats}
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.proxy.LoggingProxy


object Main {
  var state: Rowz.State = null
  var rowzServer: TSelectorServer = null
  var jobServer: TSelectorServer = null
  var shardServer: TSelectorServer = null

  var config: ConfigMap = null
  val runtime = new RuntimeEnvironment(getClass)

  def main(args: Array[String]) {
    try {
      runtime.load(args)
      config = Configgy.config
      val w3c = new W3CStats(Logger.get("w3c"), config.getList("rowz.w3c").toArray)
      state = Rowz(config, w3c)
      state.start()
      startThrift(w3c)
      println("Running.")
    } catch {
      case _ =>
        println("Exception in initialization!")
        shutdown()
    }
  }

  def startThrift(w3c: W3CStats) {
    val timeout = config("rowz.timeout_msec").toInt.milliseconds
    val executor = TSelectorServer.makeThreadPoolExecutor(config.configMap("rowz"))
    val processor = new rowz.thrift.Rowz.Processor(LoggingProxy[rowz.thrift.Rowz.Iface](Stats, w3c, "Rowz", state.rowzService))
    rowzServer = TSelectorServer("rowz", config("rowz.server_port").toInt, processor, executor, timeout)

    val jobManagerService = new JobManagerService(state.prioritizingScheduler)
    val jobProcessor = new JobManager.Processor(LoggingProxy[JobManager.Iface](Stats, w3c, "RowzJobs", jobManagerService))
    jobServer = TSelectorServer("rowz-jobs", config("rowz.job_server_port").toInt, jobProcessor, executor, timeout)

    val shardManagerService = new ShardManagerService(state.nameServer, state.copyFactory, state.prioritizingScheduler(Priority.Medium.id))
    val shardProcessor = new ShardManager.Processor(ExceptionWrappingProxy(LoggingProxy[ShardManager.Iface](Stats, w3c, "RowzShards", shardManagerService)))
    shardServer = TSelectorServer("rowz-shards", config("rowz.shard_server_port").toInt, shardProcessor, executor, timeout)

    rowzServer.serve()
    jobServer.serve()
    shardServer.serve()
  }

  def shutdown() {
    try {
      rowzServer.stop()
      jobServer.stop()
      state.shutdown()
    } finally {
      println("Exiting!")
      System.exit(0)
    }
  }
}
