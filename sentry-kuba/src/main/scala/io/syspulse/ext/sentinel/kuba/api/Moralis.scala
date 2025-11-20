package io.syspulse.ext.sentinel.kuba.api

import scala.util.Try
import com.typesafe.scalalogging.Logger
import java.time.Instant
import spray.json._

// Moralis API helper for getting block number by timestamp
object Moralis {
  private val log = Logger(getClass.getName)

  // Chain ID mapping: full chain name -> Moralis chain identifier
  // Based on https://docs.moralis.com/supported-chains
  private val chainIdMapping: Map[String, String] = Map(
    "ethereum" -> "eth",
    "eth" -> "eth",
    "polygon" -> "polygon",
    "matic" -> "polygon",
    "bsc" -> "bsc",
    "binance" -> "bsc",
    "arbitrum" -> "arbitrum",
    "arb" -> "arbitrum",
    "base" -> "base",
    "optimism" -> "optimism",
    "op" -> "optimism",
    "avalanche" -> "avalanche",
    "avax" -> "avalanche",
    "fantom" -> "fantom",
    "ftm" -> "fantom",
    "linea" -> "linea",
    "ronin" -> "ronin",
    "pulse" -> "pulse",
    "gnosis" -> "gnosis",
    "xdai" -> "gnosis",
    "moonbeam" -> "moonbeam",
    "moonriver" -> "moonriver",
    "cronos" -> "cronos"
  )

  def getBlockByTimestamp(chain: String, timestamp: Long, apiKey: String): Try[Long] = Try {
    // Map chain name to Moralis chain identifier
    val moralisChainId = chainIdMapping.getOrElse(chain.toLowerCase, chain.toLowerCase)
    log.info(s"Mapping chain '${chain}' to Moralis chain ID '${moralisChainId}'")
    // Convert timestamp to ISO 8601 format (UTC)
    val instant = Instant.ofEpochMilli(timestamp)
    val dateStr = instant.toString // Already in ISO 8601 format

    // Moralis API URL
    val urlStr = s"https://deep-index.moralis.io/api/v2.2/dateToBlock?chain=${moralisChainId}&date=${java.net.URLEncoder.encode(dateStr, "UTF-8")}"

    log.info(s"Calling Moralis API: ${urlStr}")

    // Make HTTP GET request
    val response = requests.get(
      urlStr,
      headers = Map(
        "accept" -> "application/json",
        "X-API-Key" -> apiKey
      ),
      readTimeout = 30000,
      connectTimeout = 30000
    )

    if (response.statusCode == 200) {
      // Parse JSON response to get block number
      val json = response.text().parseJson.asJsObject
      val blockNumber = json.fields.get("block") match {
        case Some(JsNumber(num)) => num.toLong
        case Some(JsString(str)) => str.toLong
        case _ => throw new Exception(s"Invalid response format: ${response.text()}")
      }

      log.info(s"Got block number ${blockNumber} for timestamp ${timestamp} (${dateStr}) on chain ${moralisChainId} (${chain})")
      blockNumber
    } else {
      throw new Exception(s"Moralis API error (${response.statusCode}): ${response.text()}")
    }
  }
}
