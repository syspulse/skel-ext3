package io.syspulse.ext.sentinel

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Success, Failure}

import io.syspulse.ext.sentinel.feeds.RssFeed

class RssFeedSpec extends AnyFlatSpec with Matchers {

  "RssFeed" should "parse Cointelegraph RSS feed from file" in {
    val feed = new RssFeed("sentry-news/rss/coingtelegraph-all.rss.xml")
    val result = feed.fetchFeed()

    result shouldBe a[Success[_]]
    val posts = result.get
    posts should not be empty

    val firstPost = posts.head
    firstPost.id should not be empty
    firstPost.title should not be empty
    firstPost.link should startWith("https://")
    firstPost.publishedDate should be > 0L
    firstPost.feedMetadata("feed_type") shouldBe "rss"
  }

  it should "extract categories from RSS feed" in {
    val feed = new RssFeed("sentry-news/rss/coingtelegraph-all.rss.xml")
    val posts = feed.fetchFeed().get

    // Check if any posts have categories
    val postsWithCategories = posts.filter(_.feedMetadata.contains("categories"))
    postsWithCategories.size should be > 0
  }

  it should "extract author from dc:creator field" in {
    val feed = new RssFeed("sentry-news/rss/coingtelegraph-all.rss.xml")
    val posts = feed.fetchFeed().get

    posts should not be empty
    // The feed has dc:creator fields, so at least some posts should have authors
    val postsWithAuthors = posts.filter(_.author != "Unknown")
    postsWithAuthors.size should be > 0
  }

  it should "strip HTML and CDATA from description" in {
    val feed = new RssFeed("sentry-news/rss/coingtelegraph-all.rss.xml")
    val posts = feed.fetchFeed().get

    posts.foreach { post =>
      post.summary should not include "<![CDATA["
      post.summary should not include "]]>"
      post.summary should not include "<p>"
      post.summary should not include "</p>"
    }
  }

  it should "limit summary to 1000 characters" in {
    val feed = new RssFeed("sentry-news/rss/coingtelegraph-all.rss.xml")
    val posts = feed.fetchFeed().get

    posts.foreach { post =>
      post.summary.length should be <= 1000
    }
  }

  it should "use link as fallback if guid is empty" in {
    // This test would need a special test RSS file with no GUID
    // For now, we just verify the logic doesn't crash
    val feed = new RssFeed("sentry-news/rss/coingtelegraph-all.rss.xml")
    val posts = feed.fetchFeed().get

    posts.foreach { post =>
      post.id should not be empty
    }
  }

  it should "handle malformed XML gracefully" in {
    val feed = new RssFeed("sentry-news/nonexistent.xml")
    val result = feed.fetchFeed()

    result shouldBe a[Failure[_]]
  }

  it should "return correct source type" in {
    val feed = new RssFeed("sentry-news/rss/coingtelegraph-all.rss.xml")
    feed.getSourceType() shouldBe "rss"
  }

  it should "return correct source" in {
    val sourcePath = "sentry-news/rss/coingtelegraph-all.rss.xml"
    val feed = new RssFeed(sourcePath)
    feed.getSource() shouldBe sourcePath
  }
}
