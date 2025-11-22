package io.syspulse.ext.sentinel

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import com.typesafe.scalalogging.Logger
import scala.util.{Try, Success, Failure}

import io.syspulse.skel.plugin.{Plugin, PluginDescriptor}
import io.syspulse.skel.blockchain.Blockchain

import io.syspulse.haas.ingest.eth.{Block}
import io.syspulse.haas.ingest.eth.etl.{Tx}

import io.hacken.ext.core.Severity
import io.hacken.ext.sentinel.SentryRun
import io.hacken.ext.sentinel.Sentry
import io.hacken.ext.sentinel.util.EventUtil
import io.hacken.ext.detector.DetectorConfig
import io.hacken.ext.core.Event
import io.hacken.ext.sentinel.ThresholdDouble

// --------------------------------------------------------------------------------------------------------------------------
object DetectorBubblemaps {
  val DEF_CRON = "10 min"
  val DEF_CONDITION = "> 0.0"
  val DEF_DESC = "Decentralization: {score}"

  val DEF_TRACK_ERR = true
  val DEF_TRACK_ERR_ALWAYS = true

  val DEF_SEV_OK = Severity.INFO
  val DEF_SEV_ERR = Severity.ERROR
}

class DetectorBubblemaps(pd: PluginDescriptor) extends Sentry with Plugin {
  override def did = pd.name
  override def toString = s"${this.getClass.getSimpleName}(${did})"

  override def getSettings(rx: SentryRun): Map[String, Any] = {
    rx.getConfig().env match {
      case "test" => Map()
      case "dev" =>
        Map("_cron_rate_limit" -> 1 * 10 * 1000L) // 10 sec
      case _ =>
        Map("_cron_rate_limit" -> 10 * 60 * 1000L) // 10 min
    }
  }

  override def onInit(rx: SentryRun, conf: DetectorConfig): Int = {
    val r = super.onInit(rx, conf)
    if (r != SentryRun.SENTRY_INIT) {
      return r
    }

    SentryRun.SENTRY_INIT
  }

  override def onStart(rx: SentryRun, conf: DetectorConfig): Int = {
    val r = onUpdate(rx, conf)
    if (r != SentryRun.SENTRY_RUNNING) {
      return r
    }

    super.onStart(rx, conf)
  }

  def mapSeverity(score: Double): Option[Double] = {
    score match {
      case s if s < 10.0 => Some(Severity.CRITICAL)
      case s if s < 30.0 => Some(Severity.HIGH)  // Major
      case s if s < 50.0 => Some(Severity.MEDIUM)
      case s if s >= 65.0 => Some(Severity.LOW)
      case _ => Some(Severity.MEDIUM)  // 50.0 - 64.9 range
    }
  }

  override def onUpdate(rx: SentryRun, conf: DetectorConfig): Int = {
    if (rx.isAddrEmpty()) {
      log.warn(s"${rx.getExtId()}: address undefined: ${rx.getAddr()}")
      error(s"Address undefined")
      return SentryRun.SENTRY_STOPPED
    }

    // Get API key from user config or from configuration
    val apiKey = {
      val key = DetectorConfig.getString(conf, "api_key", "")
      if (key.isEmpty()) {
        rx.getConfiguration()(c => c.getString("bm.api.key")).getOrElse("")
      } else {
        key
      }
    }

    if (apiKey.isEmpty()) {
      log.warn(s"${rx.getExtId()}: Bubblemaps API Key is required")
      error(s"Bubblemaps API Key is required")
      return SentryRun.SENTRY_STOPPED
    }

    // Create Bubblemaps instance with apiKey
    val bubblemaps = new Bubblemaps(apiKey)
    rx.set("bubblemaps", bubblemaps)

    // Set threshold condition
    val condition = DetectorConfig.getString(conf, "condition", DetectorBubblemaps.DEF_CONDITION)
    val threshold = new ThresholdDouble(0.0, condition)
    rx.set("threshold", threshold)

    // Set description
    rx.set("desc", DetectorConfig.getString(conf, "desc", DetectorBubblemaps.DEF_DESC))

    // Set error tracking options
    rx.set("track_err", DetectorConfig.getBoolean(conf, "track_err", DetectorBubblemaps.DEF_TRACK_ERR))
    rx.set("err_always", DetectorConfig.getBoolean(conf, "err_always", DetectorBubblemaps.DEF_TRACK_ERR_ALWAYS))

    // Initialize threshold with first query
    checkDecentralization(rx, init = true)

    SentryRun.SENTRY_RUNNING
  }

  def checkDecentralization(rx: SentryRun, init: Boolean = false): Seq[Event] = {
    val addr = rx.getAddr() match {
      case Some(addr) => addr.toLowerCase
      case None =>
        log.warn(s"${rx.getExtId()}: address undefined")
        return Seq.empty
    }

    val chain = rx.getChain().getOrElse("") match {
      case "" =>
        log.warn(s"${rx.getExtId()}: chain undefined")
        return Seq.empty
      case c => c
    }

    val bubblemaps = rx.get("bubblemaps").get.asInstanceOf[Bubblemaps]
    val threshold = rx.get("threshold").get.asInstanceOf[ThresholdDouble]

    val result = bubblemaps.getMapData(addr, chain)

    result match {
      case Success(data) =>
        val score = data.decentralization_score
        val oldScore = threshold.value()
        val changed = threshold.set(score)

        log.info(s"${rx.getExtId()}: addr=${addr}, chain=${chain}: decentralization_score: ${oldScore} -> ${score}, changed=${changed}")
        
        if (changed) {
          // Map decentralization_score to severity
          val severity = mapSeverity(score)

          // Format clusters metadata
          val clustersMetadata = data.clusters.take(10).map { cluster =>
            s"share:${cluster.share},amount:${cluster.amount},holders:${cluster.holder_count}"
          }.mkString("; ")

          // Format top 10 holders metadata
          val topHoldersMetadata = data.top_holders.take(10).map { holder =>
            val amount = holder.holder_data.map(_.amount.toString).getOrElse("0")
            s"${holder.address}:${amount}"
          }.mkString(", ")

          // Get Bubblemaps chain for link
          val bubblemapsChain = bubblemaps.mapChain(chain).getOrElse(chain)
          val link = s"https://v2.bubblemaps.io/map?address=${addr}&chain=${bubblemapsChain}"

          val desc = rx.get("desc").asInstanceOf[Option[String]].getOrElse(DetectorBubblemaps.DEF_DESC)

          val metadata = Map(
            "score" -> score.toString,
            "old" -> oldScore.toString,
            "clusters" -> clustersMetadata,
            "holders" -> topHoldersMetadata,
            "link" -> link,
            "desc" -> desc
          )

          Seq(EventUtil.createEvent(
            did,
            tx = None,
            Some(addr),
            conf = Some(rx.getConf()),
            meta = metadata,
            sev = severity
          ))
        } else {
          Seq.empty
        }

      case Failure(e) =>
        log.warn(s"${rx.getExtId()}: Failed to fetch Bubblemaps data: ${e.getMessage}")

        val trackErr = rx.get("track_err").asInstanceOf[Option[Boolean]].getOrElse(DetectorBubblemaps.DEF_TRACK_ERR)

        if (trackErr) {
          // Check if error is unique
          val always = rx.get("err_always").asInstanceOf[Option[Boolean]].getOrElse(DetectorBubblemaps.DEF_TRACK_ERR_ALWAYS)
          val lastErr = rx.get("err_last").asInstanceOf[Option[String]]
          val err = e.getMessage()

          if (always || !lastErr.isDefined || lastErr.get != err) {
            rx.set("err_last", err)

            val desc = rx.get("desc").asInstanceOf[Option[String]].getOrElse(DetectorBubblemaps.DEF_DESC)

            Seq(EventUtil.createEvent(
              did,
              tx = None,
              Some(addr),
              conf = Some(rx.getConf()),
              meta = Map(
                "err" -> err,
                "desc" -> desc
              ),
              sev = Some(DetectorBubblemaps.DEF_SEV_ERR)
            ))
          } else {
            Seq.empty
          }
        } else {
          Seq.empty
        }
    }
  }

  override def onCron(rx: SentryRun, elapsed: Long): Seq[Event] = {
    checkDecentralization(rx)
  }
}
