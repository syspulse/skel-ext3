package io.syspulse.ext.sentinel.kuba.source

import scala.util.Try
import io.syspulse.skel.blockchain.Token
import io.syspulse.ext.sentinel.Balance

trait BalanceSource {
  def getBalance(addr:String,chain:String,token:Token,block:Option[Long]):Try[Balance]
  def getBlocks(): Map[String, Long] = Map.empty // Override in implementations that cache blocks
}
