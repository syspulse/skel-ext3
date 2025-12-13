package io.syspulse.ext.sentinel

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DetectorNewsSpec extends AnyFlatSpec with Matchers {

  "DetectorNews.parseFeedUri" should "parse RSS URI when type is 'rss'" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("./rss/feed.xml", "rss")
    feedType shouldBe "rss"
    cleanedUri shouldBe "./rss/feed.xml"
  }

  it should "parse Reddit URI when type is 'reddit'" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("./reddit/feed.xml", "reddit")
    feedType shouldBe "reddit"
    cleanedUri shouldBe "./reddit/feed.xml"
  }

  it should "handle case-insensitive type" in {
    val (feedType1, _) = DetectorNews.parseFeedUri("./feed.xml", "RSS")
    feedType1 shouldBe "rss"

    val (feedType2, _) = DetectorNews.parseFeedUri("./feed.xml", "Reddit")
    feedType2 shouldBe "reddit"
  }

  it should "strip rss:// prefix when type is empty" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("rss://./rss/feed.xml", "")
    feedType shouldBe "rss"
    cleanedUri shouldBe "./rss/feed.xml"
  }

  it should "strip reddit:// prefix when type is empty" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("reddit://./reddit/feed.xml", "")
    feedType shouldBe "reddit"
    cleanedUri shouldBe "./reddit/feed.xml"
  }

  it should "assume RSS for no prefix when type is empty" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("./feed.xml", "")
    feedType shouldBe "rss"
    cleanedUri shouldBe "./feed.xml"
  }

  it should "strip rss:// prefix with http:// URI" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("rss://https://example.com/feed.rss", "")
    feedType shouldBe "rss"
    cleanedUri shouldBe "https://example.com/feed.rss"
  }

  it should "strip reddit:// prefix with http:// URI" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("reddit://https://reddit.com/domain/example.com.rss", "")
    feedType shouldBe "reddit"
    cleanedUri shouldBe "https://reddit.com/domain/example.com.rss"
  }

  it should "strip rss:// prefix with file:// URI" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("rss://file://./rss/feed.xml", "")
    feedType shouldBe "rss"
    cleanedUri shouldBe "file://./rss/feed.xml"
  }

  it should "strip reddit:// prefix with file:// URI" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("reddit://file://./reddit/feed.xml", "")
    feedType shouldBe "reddit"
    cleanedUri shouldBe "file://./reddit/feed.xml"
  }

  it should "assume RSS for unknown type" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("./feed.xml", "unknown")
    feedType shouldBe "rss"
    cleanedUri shouldBe "./feed.xml"
  }

  it should "ignore prefix when type is explicitly set to rss" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("reddit://./feed.xml", "rss")
    feedType shouldBe "rss"
    cleanedUri shouldBe "reddit://./feed.xml" // URI not cleaned, type forces RSS
  }

  it should "ignore prefix when type is explicitly set to reddit" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("rss://./feed.xml", "reddit")
    feedType shouldBe "reddit"
    cleanedUri shouldBe "rss://./feed.xml" // URI not cleaned, type forces Reddit
  }

  it should "handle multiple slashes after prefix" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("rss://https://example.com/feed.rss", "")
    feedType shouldBe "rss"
    cleanedUri shouldBe "https://example.com/feed.rss"
  }

  it should "handle empty URI with empty type" in {
    val (feedType, cleanedUri) = DetectorNews.parseFeedUri("", "")
    feedType shouldBe "rss"
    cleanedUri shouldBe ""
  }
}
