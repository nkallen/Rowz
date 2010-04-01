package com.twitter.rowz.jobs

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.jobs.UnboundJob


case class Destroy(row: Row, at: Time) extends UnboundJob[ForwardingManager] {
  def toMap = {
    Map("id" -> row.id, "name" -> row.name, "createdAt" -> row.createdAt.inSeconds, "at" -> at.inSeconds)
  }

  def apply(forwardingManager: ForwardingManager) = {
    forwardingManager(row.id).destroy(row, at)
  }
}