package io.syspulse.ext.sentinel

import scala.util.{Try, Success, Failure}
import com.typesafe.scalalogging.Logger
import spray.json._
import io.syspulse.skel.service.JsonCommon
import io.syspulse.skel.blockchain.Blockchain

case class BubblemapsCluster(
  share: Double,
  amount: Double,
  holder_count: Int,
  holders: Seq[String]
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

case class BubblemapsNodes(
  top_holders: Seq[BubblemapsTopHolder]
)

case class BubblemapsIdentifiedSupply(
  share_in_cexs: Option[Double],
  share_in_dexs: Option[Double],
  share_in_other_contracts: Option[Double]
)

case class BubblemapsMetadata(
  dt_update: String,
  ts_update: Long,
  identified_supply: Option[BubblemapsIdentifiedSupply]
)

case class BubblemapsApiResponse(
  decentralization_score: Double,
  clusters: Seq[BubblemapsCluster],
  nodes: Option[BubblemapsNodes],
  metadata: Option[BubblemapsMetadata],
  relationships: Option[JsValue]
)

object BubblemapsJsonProtocol extends DefaultJsonProtocol {
  implicit val holderDetailsFormat: RootJsonFormat[BubblemapsHolderDetails] = jsonFormat11(BubblemapsHolderDetails)
  implicit val holderDataFormat: RootJsonFormat[BubblemapsHolderData] = jsonFormat3(BubblemapsHolderData)
  implicit val topHolderFormat: RootJsonFormat[BubblemapsTopHolder] = jsonFormat4(BubblemapsTopHolder)
  implicit val clusterFormat: RootJsonFormat[BubblemapsCluster] = jsonFormat4(BubblemapsCluster)
  implicit val nodesFormat: RootJsonFormat[BubblemapsNodes] = jsonFormat1(BubblemapsNodes)
  implicit val identifiedSupplyFormat: RootJsonFormat[BubblemapsIdentifiedSupply] = jsonFormat3(BubblemapsIdentifiedSupply)
  implicit val metadataFormat: RootJsonFormat[BubblemapsMetadata] = jsonFormat3(BubblemapsMetadata)
  implicit val apiResponseFormat: RootJsonFormat[BubblemapsApiResponse] = jsonFormat5(BubblemapsApiResponse)
}

class Bubblemaps(uri: String) {
  private val log = Logger(getClass.getName)

  private val apiKey = uri
  private val BASE_URL = "https://api.bubblemaps.io"

  // Map Blockchain chain names to Bubblemaps chain identifiers
  // Based on https://docs.bubblemaps.io/data/api/endpoints/get-supported-chains
  private val chainMapping: Map[String, String] = Map(
    Blockchain.ETHEREUM.name -> "eth",
    Blockchain.BSC_MAINNET.name -> "bsc",
    Blockchain.POLYGON_MAINNET.name -> "polygon",
    Blockchain.ARBITRUM_MAINNET.name -> "arbitrum",
    Blockchain.OPTIMISM_MAINNET.name -> "optimism",
    Blockchain.AVALANCHE_MAINNET.name -> "avalanche",
    Blockchain.FANTOM_MAINNET.name -> "fantom",
    Blockchain.BASE_MAINNET.name -> "base",
    Blockchain.LINEA_MAINNET.name -> "linea",
    Blockchain.SCROLL_MAINNET.name -> "scroll",
    Blockchain.BLAST_MAINNET.name -> "blast",
    Blockchain.POLYGON_ZKEVM_MAINNET.name -> "polygon-zkevm",
    Blockchain.ZKSYNC_MAINNET.name -> "zksync"
  )

  def mapChain(blockchainChain: String): Option[String] = {
    chainMapping.get(blockchainChain.toLowerCase)
  }

  def getMapData(address: String, chain: String): Try[BubblemapsResponse] = {
    val bubblemapsChain = mapChain(chain) match {
      case Some(bmChain) => bmChain
      case None =>
        log.warn(s"Chain not supported: '${chain}'")
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
    import BubblemapsJsonProtocol._

    val apiResponse = jsonText.parseJson.convertTo[BubblemapsApiResponse]

    BubblemapsResponse(
      decentralization_score = apiResponse.decentralization_score,
      clusters = apiResponse.clusters,
      top_holders = apiResponse.nodes.map(_.top_holders).getOrElse(Seq.empty)
    )
  }
}
