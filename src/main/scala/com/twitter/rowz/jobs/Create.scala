package com.twitter.rowz.jobs

import com.twitter.gizzard.jobs.UnboundJob
import com.twitter.xrayspecs.Time


class Create(id: Long, info: RowInfo, at: Time) extends UnboundJob[ForwardingManager] {
  def toMap = {
    Map("id" -> id, "name" -> info.name, "at" -> at.inSeconds)
  }

  def apply(forwardingManager: ForwardingManager) = ()
}