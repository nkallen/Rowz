package com.twitter.rowz.jobs

import com.twitter.xrayspecs.Time
import com.twitter.gizzard.jobs.UnboundJob


class Destroy(id: Long, at: Time) extends UnboundJob[ForwardingManager] {
  def toMap = {
    Map("id" -> id, "at" -> at.inSeconds)
  }

  def apply(forwardingManager: ForwardingManager) = ()
}