package com.twitter.rowz

import com.twitter.gizzard.proxy.ExceptionHandlingProxy
import net.lag.logging.Logger


object ExceptionWrappingProxy extends ExceptionHandlingProxy({e =>
  val log = Logger.get

  log.error(e, "Error in Rowz: " + e)
  throw new thrift.RowzException(e.toString)
})

