package io.syspulse.ext.sentinel.feeds

case class NewsPost(
  id: String,                      // GUID (RSS) or entry ID (Atom)
  title: String,
  link: String,
  author: String,
  publishedDate: Long,             // Unix timestamp (ms)
  summary: String,
  source: String,
  feedMetadata: Map[String, String] // subreddit, categories, thumbnails, etc.
)
