package com.twitter.rowz.jobs

import com.twitter.gizzard.jobs.UnboundJob
import com.twitter.xrayspecs.Time


case class Create(id: Long, name: String, at: Time) extends UnboundJob[ForwardingManager] {
  def toMap = {
    Map("id" -> id, "name" -> name, "at" -> at.inSeconds)
  }

  def apply(forwardingManager: ForwardingManager) = {
    forwardingManager(id).create(id, name, at)
  }
}