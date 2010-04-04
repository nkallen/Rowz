package com.twitter.rowz.jobs

import com.twitter.gizzard.jobs.UnboundJob
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._


case class Create(id: Long, name: String, at: Time) extends UnboundJob[ForwardingManager] {
  def this(attributes: Map[String, AnyVal]) = {
    this(
      attributes("id").toLong,
      attributes("name").toString,
      Time(attributes("at").toInt.seconds))
  }

  def toMap = {
    Map("id" -> id, "name" -> name, "at" -> at.inSeconds)
  }

  def apply(forwardingManager: ForwardingManager) = {
    forwardingManager(id).create(id, name, at)
  }
}