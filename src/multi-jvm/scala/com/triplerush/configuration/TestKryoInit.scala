package com.triplerush.configuration

import com.esotericsoftware.kryo.Kryo
import com.signalcollect.configuration.KryoInit

class TestKryoInit extends KryoInit {
  override def customize(kryo: Kryo): Unit = {
    kryo.setReferences(true)
    kryo.setCopyReferences(true)
    register(kryo)
  }
}
