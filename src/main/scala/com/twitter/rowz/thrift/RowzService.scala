package com.twitter.rowz

import net.lag.configgy.Config
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import jobs.{Create, Destroy}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import thrift.conversions.Row._
import thrift.conversions.RowInfo._


class RowzService(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler, makeId: () => Long) extends thrift.Rowz.Iface {
  def create(rowInfo: thrift.RowInfo, at: Int) = {
    val id = makeId()
    scheduler(Priority.High.id)(new Create(id, rowInfo.fromThrift, Time(at.seconds)))
    id
  }

  def destroy(id: Long, at: Int) {
    scheduler(Priority.Low.id)(new Destroy(id, Time(at.seconds)))
  }

  def read(id: Long) = {
    forwardingManager(id).read(id).toThrift
  }
}