package com.twitter.rowz

import com.twitter.gizzard.shards
import com.twitter.xrayspecs.Time


object Shard {
  type Cursor = Long
  val CursorStart = -1
}

trait Shard extends shards.Shard {
  import Shard.Cursor

  def create(id: Long, name: String, at: Time)
  def destroy(row: Row, at: Time)
  def read(id: Long): Option[Row]
  def selectAll(cursor: Cursor, count: Int): (Seq[Row], Option[Cursor])
  def write(rows: Seq[Row])
}