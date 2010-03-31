package com.twitter.rowz

import com.twitter.xrayspecs.Time


class IdGenerator(workerId: Long) extends (() => Long) {
  private var sequence = 0L
  private val timestampRightShift = 10
  private val timestampBits = 42
  private val workerIdBits = 10
  private val maxWorkerId = -1L ^ (-1L << workerIdBits)
  private val sequenceBits = 12
  private val timestampLeftShift = sequenceBits + workerIdBits
  private val workerIdShift = sequenceBits
  private val sequenceMask = -1L ^ (-1L << sequenceBits)

  def apply() = nextId(Time.now.inMillis)

  private def nextId(timestamp: Long) = synchronized {
    sequence += 1
    ((timestamp >> timestampRightShift) << timestampLeftShift) |
      (workerId << workerIdShift) | (sequence & sequenceMask)
  }

}
