package com.twitter.rowz

import com.twitter.gizzard.nameserver
import com.twitter.gizzard.scheduler.JobScheduler


class CopyManager(val scheduler: JobScheduler) extends nameserver.CopyManager[Shard] {
  
}