package io.syspulse.ext.sentinel

import scala.util.{Try, Success, Failure}
import com.typesafe.scalalogging.Logger

case class BubblemapsCluster(
  share: Double,
  amount: Double,
  holder_count: Int
)

case class BubblemapsHolderDetails(
  address: String,
  label: Option[String],
  degree: Option[Int],
  is_supernode: Option[Boolean],
  is_contract: Option[Boolean],
  is_cex: Option[Boolean],
  is_dex: Option[Boolean],
  entity_id: Option[String],
  inward_relations: Option[Int],
  outward_relations: Option[Int],
  first_activity_date: Option[String]
)

case class BubblemapsHolderData(
  amount: Double,
  rank: Int,
  share: Double
)

case class BubblemapsTopHolder(
  address: String,
  address_details: Option[BubblemapsHolderDetails],
  holder_data: Option[BubblemapsHolderData],
  is_shown_on_map: Boolean
)

case class BubblemapsResponse(
  decentralization_score: Double,
  clusters: Seq[BubblemapsCluster],
  top_holders: Seq[BubblemapsTopHolder]
)

class Bubblemaps(uri: String) {
  private val log = Logger(getClass.getName)

  private val apiKey = uri
  private val BASE_URL = "https://api.bubblemaps.io"

  // Map Blockchain chain names to Bubblemaps chain identifiers
  // Based on https://docs.bubblemaps.io/data/api/endpoints/get-supported-chains
  private val chainMapping: Map[String, String] = Map(
    "ethereum" -> "eth",
    "bsc" -> "bsc",
    "polygon" -> "polygon",
    "arbitrum" -> "arbitrum",
    "optimism" -> "optimism",
    "avalanche" -> "avalanche",
    "fantom" -> "fantom",
    "base" -> "base",
    "linea" -> "linea",
    "scroll" -> "scroll",
    "blast" -> "blast",
    "polygon_zkevm" -> "polygon-zkevm",
    "zksync" -> "zksync"
  )

  def mapChain(blockchainChain: String): Option[String] = {
    chainMapping.get(blockchainChain.toLowerCase)
  }

  def getMapData(address: String, chain: String): Try[BubblemapsResponse] = {
    val bubblemapsChain = mapChain(chain) match {
      case Some(bmChain) => bmChain
      case None =>
        log.warn(s"Chain ${chain} not supported by Bubblemaps, using as-is")
        chain.toLowerCase
    }

    val url = s"${BASE_URL}/maps/${bubblemapsChain}/${address}"

    try {
      log.info(s"Fetching Bubblemaps data --> ${url}")

      val response = requests.get(
        url,
        headers = Map(
          "X-ApiKey" -> apiKey,
          "Content-Type" -> "application/json"
        ),
        readTimeout = 60000,
        connectTimeout = 30000
      )

      if (response.statusCode == 200) {
        parseResponse(response.text())
      } else {
        Failure(new Exception(s"Bubblemaps API error (${response.statusCode}): ${response.text()}"))
      }
    } catch {
      case e: Exception =>
        log.error(s"Failed to fetch Bubblemaps data: ${e.getMessage}")
        Failure(e)
    }
  }

  private def parseResponse(jsonText: String): Try[BubblemapsResponse] = Try {
    val json = ujson.read(jsonText)

    // Extract decentralization_score
    val decentralizationScore = json.obj.get("decentralization_score")
      .map(_.num)
      .getOrElse(0.0)

    // Extract clusters
    val clusters = json.obj.get("clusters") match {
      case Some(clustersJson) if clustersJson.arr.nonEmpty =>
        clustersJson.arr.map { cluster =>
          BubblemapsCluster(
            share = cluster.obj.get("share").map(_.num).getOrElse(0.0),
            amount = cluster.obj.get("amount").map(_.num).getOrElse(0.0),
            holder_count = cluster.obj.get("holder_count").map(_.num.toInt).getOrElse(0)
          )
        }.toSeq
      case _ => Seq.empty
    }

    // Extract top holders
    val topHolders = json.obj.get("nodes") match {
      case Some(nodes) =>
        nodes.obj.get("top_holders") match {
          case Some(holders) if holders.arr.nonEmpty =>
            holders.arr.map { holder =>
              val address = holder.obj.get("address").map(_.str).getOrElse("")

              val addressDetails = holder.obj.get("address_details").map { details =>
                BubblemapsHolderDetails(
                  address = address,
                  label = details.obj.get("label").map(_.str),
                  degree = details.obj.get("degree").map(_.num.toInt),
                  is_supernode = details.obj.get("is_supernode").map(_.bool),
                  is_contract = details.obj.get("is_contract").map(_.bool),
                  is_cex = details.obj.get("is_cex").map(_.bool),
                  is_dex = details.obj.get("is_dex").map(_.bool),
                  entity_id = details.obj.get("entity_id").map(_.str),
                  inward_relations = details.obj.get("inward_relations").map(_.num.toInt),
                  outward_relations = details.obj.get("outward_relations").map(_.num.toInt),
                  first_activity_date = details.obj.get("first_activity_date").map(_.str)
                )
              }

              val holderData = holder.obj.get("holder_data").map { data =>
                BubblemapsHolderData(
                  amount = data.obj.get("amount").map(_.num).getOrElse(0.0),
                  rank = data.obj.get("rank").map(_.num.toInt).getOrElse(0),
                  share = data.obj.get("share").map(_.num).getOrElse(0.0)
                )
              }

              val isShownOnMap = holder.obj.get("is_shown_on_map").map(_.bool).getOrElse(false)

              BubblemapsTopHolder(
                address = address,
                address_details = addressDetails,
                holder_data = holderData,
                is_shown_on_map = isShownOnMap
              )
            }.toSeq
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }

    BubblemapsResponse(
      decentralization_score = decentralizationScore,
      clusters = clusters,
      top_holders = topHolders
    )
  }
}
