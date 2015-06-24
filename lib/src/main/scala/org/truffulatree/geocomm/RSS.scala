package org.truffulatree.geocomm

import scalaz._
import Scalaz._
import scala.xml

case class RSSChannel(
  title: String,
  link: Option[String], // should be URI
  description: Option[String],
  pubDate: Option[String], // should be Date
  content: Seq[RSSItem])

case class RSSItem(
  title: String,
  link: Option[String], // should be URI
  description: Option[String],
  pubDate: Option[String], // should be date
  content: xml.NodeSeq)

object RSS {
  def parse(rss: xml.Node): \/[Throwable, RSSChannel] =
    if (rss.label == "rss")
      \/.fromTryCatchNonFatal {
        val channel = (rss \ "channel").head
        val title = (channel \ "title").head.text
        val link = (channel \ "link").headOption.map(_.text)
        val description = (channel \ "description").headOption.map(_.text)
        val pubDate = (channel \ "pubDate").headOption.map(_.text)
        val rssItems = ((channel \ "item") map (parseItem _)).toList.sequenceU
        rssItems map (r => RSSChannel(title, link, description, pubDate, r))
      } flatMap (identity)
      else (new Exception("Not an RSS document")).left

  def parseItem(item: xml.Node): \/[Throwable, RSSItem] =
    \/.fromTryCatchNonFatal {
      val title = (item \ "title").head.text
      val link = (item \ "link").headOption.map(_.text)
      val description = (item \ "description").headOption.map(_.text)
      val pubDate = (item \ "pubDate").headOption.map(_.text)
      val content = item.child.filterNot(
        ch => Set("title", "link", "description", "pubDate") contains ch.label)
      RSSItem(title, link, description, pubDate, content)
    }
}
