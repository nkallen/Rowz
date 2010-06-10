package com.twitter.rowz

import net.lag.configgy.Configgy
import org.specs.Specification

abstract class ConfiguredSpecification extends Specification {
  try {
  Configgy.configure("config/test.conf")
  } catch {
    case ex: Exception => ex.printStackTrace()
  }

  lazy val config = Configgy.config
}

