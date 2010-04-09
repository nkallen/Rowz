package com.twitter.rowz.jobs

import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.jobs.UnboundJob


object DestroyParser extends gizzard.jobs.UnboundJobParser[ForwardingManager] {
  def apply(attributes: Map[String, Any]) = {
    new Destroy(
      new Row(
        attributes("id").asInstanceOf[Long],
        attributes("name").asInstanceOf[String],
        Time(attributes("createdAt").asInstanceOf[Int].seconds),
        Time(attributes("updatedAt").asInstanceOf[Int].seconds),
        State(attributes("state").asInstanceOf[Int])),
        Time(attributes("at").asInstanceOf[Int].seconds))
  }
}

case class Destroy(row: Row, at: Time) extends UnboundJob[ForwardingManager] {
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