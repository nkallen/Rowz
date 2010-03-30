package com.twitter.rowz.jobs

import com.twitter.xrayspecs.Time
import com.twitter.gizzard.jobs.UnboundJob


class Destroy(id: Long, at: Time) extends UnboundJob[ForwardingManager] {
  
}