package io.syspulse.ext.sentinel

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import scala.io.Source

class BubblemapsSpec extends AnyWordSpec with Matchers {

  "BubblemapsJsonProtocol" should {
    "parse SHIB-like response correctly" in {
      import BubblemapsJsonProtocol._

      val jsonText = """{"metadata":{"dt_update":"2025-11-21T02:37:30.727236","ts_update":1763692650,"identified_supply":{"share_in_cexs":0.25,"share_in_dexs":0.0,"share_in_other_contracts":0.42}},"nodes":null,"relationships":null,"decentralization_score":57.51,"clusters":[{"share":0.03490022082813747,"amount":34899604348114.22,"holder_count":13,"holders":["0x2d7af085f2256f114c8a9f540969f0e0ab1c2e5e","0xe1474359c74e78fa8387d9cb58f393693e378de3"]}]}"""

      val apiResponse = jsonText.parseJson.convertTo[BubblemapsApiResponse]

      apiResponse.decentralization_score shouldBe 57.51
      apiResponse.clusters should have length 1
      apiResponse.nodes shouldBe None
      apiResponse.metadata shouldBe defined

      val firstCluster = apiResponse.clusters.head
      firstCluster.share shouldBe 0.03490022082813747 +- 0.0001
      firstCluster.holder_count shouldBe 13
      firstCluster.holders should have length 2
      firstCluster.holders.head shouldBe "0x2d7af085f2256f114c8a9f540969f0e0ab1c2e5e"
    }

    "handle null nodes field" in {
      import BubblemapsJsonProtocol._

      val json = """{"decentralization_score":50.0,"clusters":[],"nodes":null,"metadata":null,"relationships":null}"""
      val apiResponse = json.parseJson.convertTo[BubblemapsApiResponse]

      apiResponse.nodes shouldBe None
      apiResponse.decentralization_score shouldBe 50.0
    }
  }
}
