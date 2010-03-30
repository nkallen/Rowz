package com.twitter.rowz

import net.lag.configgy.Config
import com.twitter.gizzard.scheduler.PrioritizingJobScheduler
import jobs.{Create, Destroy}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import thrift.conversions.Row._
import thrift.conversions.RowInfo._


class RowzService(forwardingManager: ForwardingManager, scheduler: PrioritizingJobScheduler) extends thrift.Rowz.Iface {
  def create(rowInfo: RowInfo, at: Int) = {
/*    val id = makeId()*/
    val id = 1
    scheduler(0)(new Create(id, rowInfo, Time(at.seconds))) // XXX priority const
    id
  }

  def destroy(id: Long, at: Int) {
    scheduler(1)(new Destroy(id, Time(at.seconds))) // XXX
  }

  def read(id: Long) = {
    forwardingManager(id).read(id).toThrift
  }
}