package com.triplerush

import java.util.UUID


object TestConfig {
  def prefix = UUID.randomUUID().toString.replace("-", "")
}
