package com.twitter.rowz.jobs

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.jobs.UnboundJob


case class Destroy(row: Row, at: Time) extends UnboundJob[ForwardingManager] {
  def this(attributes: Map[String, AnyVal]) = {
    this(
      new Row(
        attributes("id").toLong,
        attributes("name").toString,
        Time(attributes("createdAt").toInt.seconds),
        Time(attributes("updatedAt").toInt.seconds),
        State(attributes("state").toInt)),
        Time(attributes("at").toInt.seconds))
  }

  def toMap = {
    Map(
      "id" -> row.id,
      "name" -> row.name,
      "createdAt" -> row.createdAt.inSeconds,
      "updatedAt" -> row.updatedAt.inSeconds,
      "state" -> row.state.id,
      "at" -> at.inSeconds)
  }

  def apply(forwardingManager: ForwardingManager) = {
    forwardingManager(row.id).destroy(row, at)
  }
}