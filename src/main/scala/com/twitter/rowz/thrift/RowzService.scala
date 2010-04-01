package com.twitter.rowz

import net.lag.configgy.Config
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import jobs.{Create, Destroy}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import thrift.conversions.Row._


class RowzService(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, makeId: () => Long) extends thrift.Rowz.Iface {
  def create(name: String, at: Int) = {
    val id = makeId()
    scheduler(Priority.High.id)(new Create(id, name, Time(at.seconds)))
    id
  }

  def destroy(row: thrift.Row, at: Int) {
    scheduler(Priority.Low.id)(new Destroy(row.fromThrift, Time(at.seconds)))
  }

  def read(id: Long) = {
    forwardingManager(id).read(id).get.toThrift
  }
}