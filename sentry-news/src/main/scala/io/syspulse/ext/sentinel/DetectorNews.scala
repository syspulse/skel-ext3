package io.syspulse.ext.sentinel

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import com.typesafe.scalalogging.Logger
import scala.util.{Try, Success, Failure}
import scala.util.matching.Regex

import io.syspulse.skel.plugin.{Plugin, PluginDescriptor}

import io.hacken.ext.core.Severity
import io.hacken.ext.sentinel.SentryRun
import io.hacken.ext.sentinel.Sentry
import io.hacken.ext.sentinel.util.EventUtil
import io.hacken.ext.detector.DetectorConfig
import io.hacken.ext.core.Event

import io.syspulse.ext.sentinel.feeds._

object DetectorNews {
  val DEF_CRON = "10 minutes"
  val DEF_FEEDS = ""
  val DEF_DESC = "New post: {title}{err}"
  val DEF_FILTER = ""  // Empty = match everything (no filtering)
  val DEF_MAX_SEEN_POSTS = 100

  val DEF_TRACK_ERR = true
  val DEF_TRACK_ERR_ALWAYS = true

  val DEF_SEV_NEW_POST = Severity.INFO
  val DEF_SEV_ERR = Severity.ERROR
}

class DetectorNews(pd: PluginDescriptor) extends Sentry with Plugin {
  override def did = pd.name
  override def toString = s"${this.getClass.getSimpleName}(${did})"

  override def getSettings(rx: SentryRun): Map[String, Any] = {
    rx.getConfig().env match {
      case "test" => Map()
      case "dev" =>
        Map("_cron_rate_limit" -> 1 * 60 * 1000L) // 1 min for dev
      case _ =>
        Map("_cron_rate_limit" -> 10 * 60 * 1000L) // 10 min for prod
    }
  }

  override def onInit(rx: SentryRun, conf: DetectorConfig): Int = {
    val r = super.onInit(rx, conf)
    if (r != SentryRun.SENTRY_INIT) {
      return r
    }

    // Initialize seen posts set
    rx.set("seen_posts", Set.empty[String])

    SentryRun.SENTRY_INIT
  }

  override def onStart(rx: SentryRun, conf: DetectorConfig): Int = {
    val r = onUpdate(rx, conf)
    if (r != SentryRun.SENTRY_RUNNING) {
      return r
    }

    super.onStart(rx, conf)
  }

  override def onUpdate(rx: SentryRun, conf: DetectorConfig): Int = {
    // Parse feeds configuration
    val feedsStr = DetectorConfig.getString(conf, "feeds", DetectorNews.DEF_FEEDS)
    if (feedsStr.isEmpty) {
      log.warn(s"${rx.getExtId()}: feeds configuration required")
      error("Feeds configuration required", None)
      return SentryRun.SENTRY_STOPPED
    }

    // Create feed instances based on URI patterns
    val feeds: Seq[NewsFeed] = feedsStr.split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { uri =>
        if (uri.contains("reddit") || uri.contains("/domain/")) {
          new RedditFeed(uri)
        } else {
          new RssFeed(uri)
        }
      }      

    log.info(s"${rx.getExtId()}: Configured feeds: ${feeds.size} (${feeds})")
    rx.set("feeds", feeds)

    // Configuration
    rx.set("desc", DetectorConfig.getString(conf, "desc", DetectorNews.DEF_DESC))

    // Regexp filter: empty string = match everything (no filtering)
    val filter = DetectorConfig.getString(conf, "filter", DetectorNews.DEF_FILTER)
    if (filter.nonEmpty) {
      try {
        val regexp = filter.r
        rx.set("regexp", Some(regexp))
      } catch {
        case e: Exception =>
          log.warn(s"${rx.getExtId()}: Invalid regexp pattern: ${filter}: ${e.getMessage}")
          error(s"Invalid regexp pattern: ${filter}", None)
          return SentryRun.SENTRY_STOPPED
      }
    } else {
      rx.set("regexp", None)
    }

    rx.set("max_seen_posts",
           DetectorConfig.getInt(conf, "max_seen_posts", DetectorNews.DEF_MAX_SEEN_POSTS))
    rx.set("track_err",
           DetectorConfig.getBoolean(conf, "track_err", DetectorNews.DEF_TRACK_ERR))
    rx.set("err_always",
           DetectorConfig.getBoolean(conf, "err_always", DetectorNews.DEF_TRACK_ERR_ALWAYS))

    // Initialize seen posts with current feed state (don't alert on first run)
    //initializeSeenPosts(rx)

    SentryRun.SENTRY_RUNNING
  }

  private def initializeSeenPosts(rx: SentryRun): Unit = {
    val feeds = rx.get("feeds").get.asInstanceOf[Seq[NewsFeed]]

    val currentPosts = feeds.flatMap { feed =>
      feed.fetchFeed() match {
        case Success(posts) => posts
        case Failure(e) =>
          log.warn(s"${rx.getExtId()}: Failed to initialize: ${feed.getSource()}: ${e.getMessage}")
          Seq.empty
      }
    }

    val postIds = currentPosts.map(_.id).toSet
    rx.set("seen_posts", postIds)
    log.info(s"${rx.getExtId()}: Initialized: ${postIds.size} (seen posts)")
  }

  override def onCron(rx: SentryRun, elapsed: Long): Seq[Event] = {
    checkFeeds(rx)
  }

  def checkFeeds(rx: SentryRun): Seq[Event] = {
    val feeds = rx.get("feeds").get.asInstanceOf[Seq[NewsFeed]]
    val seenPosts = rx.get("seen_posts").get.asInstanceOf[Set[String]]
    val maxSeenPosts = rx.get("max_seen_posts").asInstanceOf[Option[Int]].getOrElse(DetectorNews.DEF_MAX_SEEN_POSTS)

    var errorEvents = Seq.empty[Event]

    log.info(s"${rx.getExtId()}: Feed: ${feeds}")

    // Fetch all posts from all feeds
    val allPosts = feeds.flatMap { feed =>
      feed.fetchFeed() match {
        case Success(posts) =>
          log.info(s"${rx.getExtId()}: Feed: ${feed.getSource()}: ${posts.size}")
          posts
        case Failure(e) =>
          log.warn(s"${rx.getExtId()}: Failed to fetch from ${feed.getSource()}: ${e.getMessage}")
          errorEvents = errorEvents ++ handleFeedError(rx, feed, e)
          Seq.empty
      }
    }

    // Filter for new posts
    val newPosts = allPosts.filterNot(p => seenPosts.contains(p.id))

    // Apply regexp filter if configured
    val filteredPosts = filterByRegexp(rx, newPosts)

    log.info(s"${rx.getExtId()}: Posts: ${newPosts.size} (new), ${filteredPosts.size} (filtered)")

    // Update seen posts with size limit
    val allPostIds = allPosts.map(_.id).toSet
    val updatedSeen = (seenPosts ++ allPostIds).takeRight(maxSeenPosts)
    rx.set("seen_posts", updatedSeen)

    // Generate alerts for new filtered posts
    val postEvents = filteredPosts.map(post => createPostAlert(rx, post))

    errorEvents ++ postEvents
  }

  private def filterByRegexp(rx: SentryRun, posts: Seq[NewsPost]): Seq[NewsPost] = {
    val regexpOpt = rx.get("regexp").asInstanceOf[Option[Option[Regex]]].flatten

    // If no regexp configured (empty string), match everything
    if (regexpOpt.isEmpty) return posts

    val regexp = regexpOpt.get

    posts.filter { post =>
      val searchText = s"${post.title} ${post.summary}"
      regexp.findFirstIn(searchText).isDefined
    }
  }

  private def createPostAlert(rx: SentryRun, post: NewsPost): Event = {
    val desc = rx.get("desc").asInstanceOf[Option[String]].getOrElse(DetectorNews.DEF_DESC)

    val metadata = Map(
      "id" -> post.id,
      "title" -> post.title,
      "link" -> post.link,
      "author" -> post.author,
      "date" -> DateParser.formatTimestamp(post.publishedDate),
      "summary" -> post.summary,
      "src" -> post.source,      
      "desc" -> desc.replace("{title}", post.title)
    ) ++ post.feedMetadata

    EventUtil.createEvent(
      did,
      tx = None,
      monitoredAddr = None,
      conf = Some(rx.getConf()),
      meta = metadata,
      detectorTs = post.publishedDate.toString,
      sev = Some(DetectorNews.DEF_SEV_NEW_POST)
    )
  }

  private def handleFeedError(rx: SentryRun, feed: NewsFeed, error: Throwable): Seq[Event] = {
    val trackErr = rx.get("track_err").asInstanceOf[Option[Boolean]].getOrElse(DetectorNews.DEF_TRACK_ERR)

    if (!trackErr) return Seq.empty

    val always = rx.get("err_always").asInstanceOf[Option[Boolean]].getOrElse(DetectorNews.DEF_TRACK_ERR_ALWAYS)
    val errorKey = s"err_last_${feed.getSourceType()}_${feed.getSource().hashCode}"
    val lastErr = rx.get(errorKey).asInstanceOf[Option[String]]
    val errMsg = error.getMessage()

    if (always || lastErr.isEmpty || lastErr.get != errMsg) {
      rx.set(errorKey, errMsg)
      val desc = rx.get("desc").asInstanceOf[Option[String]].getOrElse(DetectorNews.DEF_DESC)

      Seq(EventUtil.createEvent(
        did,
        tx = None,
        monitoredAddr = None,
        conf = Some(rx.getConf()),
        meta = Map(
          "err" -> errMsg,
          "type" -> feed.getSourceType(),
          "src" -> feed.getSource(),
          "desc" -> desc
        ),
        sev = Some(DetectorNews.DEF_SEV_ERR)
      ))
    } else {
      Seq.empty
    }
  }
}
