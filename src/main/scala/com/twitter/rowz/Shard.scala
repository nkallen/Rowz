package com.twitter.rowz

import com.twitter.gizzard.shards
import com.twitter.xrayspecs.Time


trait Shard extends shards.Shard {
  def create(info: RowInfo, at: Time)
  def destroy(id: Long, at: Time)
  def read(id: Long): RowInfo
}