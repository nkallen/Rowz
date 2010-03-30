package com.twitter.rowz

import java.security.MessageDigest
import java.nio.{ByteBuffer, ByteOrder}


object Hash extends (Long => Long) {
  def apply(n: Long) = {
    val buffer = new Array[Byte](8)
    val byteBuffer = ByteBuffer.wrap(buffer)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    byteBuffer.putLong(n)
    val digest = MessageDigest.getInstance("SHA")
    digest.update(buffer)
    val longs = ByteBuffer.wrap(digest.digest).asLongBuffer
    val results = new Array[Long](longs.limit)
    longs.get(results)
    results.first
  }
}