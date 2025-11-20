package io.syspulse.ext.sentinel.kuba.source

import scala.util.Try

import io.syspulse.skel.blockchain.Token
import io.syspulse.ext.sentinel.Balance

class BalanceSourceDune(apiKey: String, snapshotTs: Long) extends BalanceSource {

  // TODO: Calculate blocks for all chains at snapshot timestamp
  // private val blocks: Map[String, Long] = queryBlocks(snapshotTs)

  def getBalance(addr:String,chain:String,token:Token,block:Option[Long]):Try[Balance] = {
    Try {
      // TODO: Implement Dune Analytics API query
      // Query historical balances from Dune at specific block
      // Use block if provided, otherwise use cached block for the chain
      val value = BigInt(0)
      val amount = 0.0
      Balance(addr, chain, token.sym, token.addr, value, amount, block, snapshotTs, None)
    }
  }
}
