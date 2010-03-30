package com.twitter.rowz

import net.lag.configgy.Config
import com.twitter.gizzard.nameserver.NameServer
import com.twitter.gizzard.scheduler.JobScheduler


class RowzService(nameServer: NameServer[Shard], forwardingManager: ForwardingManager, scheduler: JobScheduler) {
  def create(rowInfo: RowInfo, at: Int) = {
    val id = makeId()
    scheduler(new Create(id, rowInfo.fromThrift, at))
    id
  }

  def delete(rowInfo: RowInfo, at: Int) {
    scheduler(new Delete(id, rowInfo.fromThrift, at))
  }

  def get(id: Long) = {
    nameServer.find(id).get(id)
  }
}