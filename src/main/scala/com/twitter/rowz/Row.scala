package com.twitter.rowz

import com.twitter.xrayspecs.Time


case class Row(id: Long, name: String, createdAt: Time, updatedAt: Time, state: State.Value)