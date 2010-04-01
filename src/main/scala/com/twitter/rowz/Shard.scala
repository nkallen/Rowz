package com.twitter.rowz

import com.twitter.gizzard.shards
import com.twitter.xrayspecs.Time


trait Shard extends shards.Shard {
  def create(id: Long, name: String, at: Time)
  def destroy(row: Row, at: Time)
  def read(id: Long): Option[Row]
}