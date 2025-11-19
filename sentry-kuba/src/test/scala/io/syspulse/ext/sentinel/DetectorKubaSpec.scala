package io.syspulse.ext.sentinel

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Try, Success, Failure}
import java.math.BigInteger
import scala.collection.mutable
import spray.json._

import io.hacken.ext.detector.DetectorConfig
import io.hacken.ext.sentinel.SentryRun
import io.syspulse.haas.ingest.eth.etl.Tx
import io.syspulse.haas.ingest.eth.etl.Block
import io.syspulse.haas.ingest.eth.etl.LogTx
import io.syspulse.skel.plugin.PluginDescriptor
import io.hacken.ext.sentinel.util.EventUtil
import io.hacken.ext.sentinel.util.TokenData
import io.syspulse.skel.blockchain.eth.EthUtil
import io.hacken.ext.core.Event
import io.hacken.ext.sentinel.Config

class DetectorPoRSpec extends AnyFlatSpec with Matchers {
  
  
}