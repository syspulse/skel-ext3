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

  // Helper method to create a test transaction
  def createTestTx(
    from: String = "0x1111111111111111111111111111111111111111",
    to: Option[String] = Some("0x2222222222222222222222222222222222222222"),
    value: BigInt = BigInt(1000000000000000000L), // 1 ETH in wei
    logs: Array[LogTx] = Array.empty,
    blockNumber: Long = 12345L,
    timestamp: Long = System.currentTimeMillis()
  ): Tx = {
    Tx(
      hash = "0xtest123",
      nonce = BigInt(1),
      transaction_index = 0,
      from_address = from,
      to_address = to,
      value = value,
      gas = 21000L,
      gas_price = BigInt(20000000000L), // 20 gwei
      input = "0x",
      max_fee_per_gas = Some(BigInt(20000000000L)),
      max_priority_fee_per_gas = Some(BigInt(2000000000L)),
      transaction_type = Some(2),
      receipt_cumulative_gas_used = 21000L,
      receipt_gas_used = 21000L,
      receipt_contract_address = None,
      receipt_root = Some("0xreceipt123"),
      receipt_status = Some(1),
      receipt_effective_gas_price = Some(BigInt(20000000000L)),
      block = Block(
        number = blockNumber,
        hash = "0xblock123",
        timestamp = timestamp,
        difficulty = BigInt(0),
        gas_limit = 30000000L,
        gas_used = 21000L,
        base_fee_per_gas = Some(20000000000L),
        extra_data = "0x",
        logs_bloom = "0x",
        miner = "0xminer123",
        nonce = Some("0x"),
        parent_hash = "0xparent123",
        receipts_root = "0xreceipts123",
        sha3_uncles = "0xuncles123",
        size = 1000L,
        state_root = "0xstate123",
        total_difficulty = Some(BigInt(0)),
        transaction_count = 1,
        transactions_root = "0xtx123"
      ),
      logs = logs,
      sim = None
    )
  }

  // Helper method to create ERC20 transfer log
  def createTransferLog(
    from: String = "0x1111111111111111111111111111111111111111",
    to: String = "0x2222222222222222222222222222222222222222",
    value: BigInt = BigInt(1000000000000000000L), // 1 token with 18 decimals
    tokenAddress: String = "0xcccccccccccccccccccccccccccccccccccc0001"
  ): LogTx = {
    val transferTopic = EthUtil.EVENT_TRANSFER
    val fromTopic = "0x000000000000000000000000" + from.drop(2)
    val toTopic = "0x000000000000000000000000" + to.drop(2)
    val valueData = "0x" + ("0" * 64 + value.toString(16)).takeRight(64)
    
    LogTx(
      address = tokenAddress,
      data = valueData,
      topics = Array(transferTopic, fromTopic, toTopic),
      index = 0
    )
  }

  // Helper method to create a test SentryRun
  def createTestSentryRun(
    detector: DetectorPoR,
    config: Config,
    addr: Option[String] = None,
    chain: Option[String] = Some("ethereum"),
    tokens: Map[String, TokenData] = Map.empty,
    trackFrom: Boolean = true,
    trackTo: Boolean = true,
    trackAddr: Boolean = true,
    filterFrom: String = "",
    filterTo: String = "",
    additionalTokens: Seq[JsObject] = Seq.empty
  ): SentryRun = {
    val conf = DetectorConfig(
      id = 1,
      createdAt = System.currentTimeMillis(),
      updatedAt = System.currentTimeMillis(),
      status = "active",
      contract = io.hacken.ext.detector.DetectorConfigContract(
        id = 1,
        createdAt = System.currentTimeMillis(),
        updatedAt = System.currentTimeMillis(),
        projectId = 1,
        tenantId = 1,
        chainUid = Some("ethereum"),
        proxyAddress = None,
        implementation = None,
        address = addr,
        name = "test-contract"
      ),
      schema = None,
      name = "Test Whale Detector",
      source = "test",
      tags = Seq.empty,
      config = Some(JsObject(
        "tokens" -> JsArray(
          JsObject(
            "address" -> JsString("ETH"),
            "threshold" -> JsString(">1.0"), // 1 ETH using decimal format
            "decimals" -> JsNumber(18),
            "symbol" -> JsString("ETH")
          )
        ),
        "track_from" -> JsBoolean(trackFrom),
        "track_to" -> JsBoolean(trackTo),
        "track_addr" -> JsBoolean(trackAddr),
        "from" -> JsString(filterFrom),
        "to" -> JsString(filterTo)
      )),
      destinations = Seq.empty
    )
    
    val sentryRun = new SentryRun(detector, conf, config, None)
    // sentryRun.set("track_from", trackFrom)
    // sentryRun.set("track_addr", trackAddr)    
    sentryRun
  }

  "createTransferLog helper method" should "correctly convert BigInt values to hex with left-padding to 32 bytes" in {
    // Test various BigInt values to ensure hex conversion works correctly
    val testValues = Seq(
      BigInt(0L),                                    // Zero
      BigInt(1L),                                    // Small value
      BigInt(1000L),                                 // Medium value
      BigInt(1000000000000000000L),                  // 1 ETH in wei
      BigInt(2000000000L),                           // 2000 USDT (6 decimals)
      BigInt("300000000000000000000"),               // Example from user: 300000000000000000000
      BigInt("123456789012345678901234567890"),      // Very large value
      BigInt("999999999999999999999999999999")       // Another very large value
    )
    
    val tokenAddress = "0xcccccccccccccccccccccccccccccccccccc9999"
    
    testValues.foreach { originalValue =>
      // Create transfer log with the original value
      val transferLog = createTransferLog(
        from = "0x1111111111111111111111111111111111111111",
        to = "0x2222222222222222222222222222222222222222",
        value = originalValue,
        tokenAddress = tokenAddress
      )
      
      // Extract the hex value from the log data
      val hexValue = transferLog.data
      
      // Verify the hex is properly left-padded to 64 characters (32 bytes)
      hexValue should startWith("0x")
      hexValue.length shouldBe 66 // "0x" + 64 hex characters
      
      // Convert hex back to BigInt
      val convertedValue = BigInt(hexValue.drop(2), 16) // Remove "0x" prefix
      
      // Verify the conversion is correct
      convertedValue shouldBe originalValue
      
      // Also verify the log structure is correct
      transferLog.address shouldBe tokenAddress
      transferLog.topics should have length 3
      transferLog.topics(0) shouldBe EthUtil.EVENT_TRANSFER
    }
  }

  
}