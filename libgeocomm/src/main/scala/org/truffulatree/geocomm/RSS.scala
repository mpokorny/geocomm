package org.truffulatree.geocomm

import java.net.URI
import scalaz._
import Scalaz._
import scala.xml

case class RSSChannel(
  title: String,
  link: Option[URI],
  description: Option[String],
  pubDate: Option[String], // should be Date
  content: Seq[RSSItem])

case class RSSItem(
  title: String,
  link: Option[URI],
  description: Option[String],
  pubDate: Option[String], // should be date
  content: xml.NodeSeq)

object RSS {

  private[this] val channelTag = "channel"
  private[this] val titleTag = "title"
  private[this] val linkTag = "link"
  private[this] val descriptionTag = "description"
  private[this] val pubDateTag = "pubDate"
  private[this] val itemTag = "item"

  def parse(rss: xml.Node): \/[Throwable, RSSChannel] =
    if (rss.label == "rss")
      \/.fromTryCatchNonFatal {
        val channel = (rss \ channelTag).head
        val title = (channel \ titleTag).head.text
        val link = (channel \ linkTag).headOption.map(c => new URI(c.text))
        val description = (channel \ descriptionTag).headOption.map(_.text)
        val pubDate = (channel \ pubDateTag).headOption.map(_.text)
        val rssItems = ((channel \ itemTag) map (parseItem _)).toList.sequenceU
        rssItems map (r => RSSChannel(title, link, description, pubDate, r))
      } flatMap (identity)
      else (new Exception("Not an RSS document")).left

  def parseItem(item: xml.Node): \/[Throwable, RSSItem] =
    \/.fromTryCatchNonFatal {
      val title = (item \ titleTag).head.text
      val link = (item \ linkTag).headOption.map(c => new URI(c.text))
      val description = (item \ descriptionTag).headOption.map(_.text)
      val pubDate = (item \ pubDateTag).headOption.map(_.text)
      val content = item.child.filterNot(ch =>
        Set(titleTag, linkTag, descriptionTag, pubDateTag) contains ch.label)
      RSSItem(title, link, description, pubDate, content)
    }
}
