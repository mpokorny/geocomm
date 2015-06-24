package org.truffulatree.geocomm

import java.util.Date
import scala.xml
import scalaz._
import Scalaz._
import dispatch._, Defaults._

object TownshipGeocoder {

  val townshipGeocoder = url(
    "http://www.geocommunicator.gov/TownshipGeoCoder/TownshipGeoCoder.asmx/GetLatLon")

  def trsProps(trs: TRS): String = {
    List(
      trs.state.toString,
      trs.principalMeridian.id.shows,
      trs.townshipNumber.shows,
      trs.townshipFraction.id.shows,
      trs.townshipDirection.toString,
      trs.rangeNumber.shows,
      trs.rangeFraction.id.shows,
      trs.rangeDirection.toString,
      trs.sectionNumber.id.shows,
      trs.sectionDivision.reverse.mkString(""),
      trs.townshipDuplicate.shows).mkString(",")
  }

  def request(trs: TRS): Future[\/[Throwable, xml.Elem]] = {
    val query = townshipGeocoder <<? Map("TRS" -> trsProps(trs))
    try {
      Http(query OK as.xml.Elem) map (_.right)
    } catch {
      case e: Exception => Future(e.left)
    }
  }

  val getData: (xml.Elem) => \/[Throwable, xml.Elem] = elem =>
    if (elem.label == "TownshipGeocoderResult")
      \/.fromTryCatchNonFatal {
        (elem \ "CompletionStatus").text match {
          case "false" =>
            (new Exception((elem \ "Message").text)).left
          case "true" =>
            xml.XML.loadString((elem \ "Data").text).right
        }
      } flatMap (identity)
    else (new Exception("Unexpected response")).left

  val parseRss: (xml.Elem) => \/[Throwable, RSSChannel] =
    rss.parse _

  val selectTownshipGeoCoder: (RSSChannel) => \/[Throwable, RSSChannel] = ch =>
    if (ch.title == "Township GeoCoder") ch.right
    else (new Exception("'Township GeoCoder' element not found")).left

  def selectGeoCoderItem(itemName: String):
      (RSSChannel) => \/[Throwable, RSSItem] =
    ch => {
      val item = ch.content filter (_.title == itemName)
      \/.fromEither(
        Either.cond(
          !item.isEmpty,
          item.head,
          new Exception(s"'$itemName' item not found")))
    }

  val selectLatLon: (RSSChannel) => \/[Throwable, RSSItem] =
    selectGeoCoderItem("Lat Lon")

  val parseLatLon: (RSSItem) => \/[Throwable, (Double, Double)] = ch =>
    (ch.content filter { node =>
      node.label == "point" && node.namespace == "http://www.georss.org/georss"
    } map { node =>
      \/.fromTryCatchNonFatal {
        node.text.split(' ').toList map (_.toDouble) match {
          case List(lon, lat) => (lat, lon)
        }
      }
    }).headOption.getOrElse((new Exception("Failed to find 'point' item")).left)

  val getTownshipGeoCoder = 
    getData(_: xml.Elem) >>= parseRss >>= selectTownshipGeoCoder

  val extractLatLon = selectLatLon(_: RSSChannel) >>= parseLatLon

  val selectTownshipRangeSection: (RSSChannel) => \/[Throwable, RSSItem] =
    selectGeoCoderItem("Township Range Section")

  val parseTownshipRangeSection:
      (RSSItem) => \/[Throwable, List[(Double, Double)]] = ch => {
    (ch.content filter { node =>
      node.label == "polygon" &&
      node.namespace == "http://www.georss.org/georss"
    } map { node =>
      \/.fromTryCatchNonFatal {
        (node.text.split(',').toList map (_.toDouble) grouped(2) map {
          case List(lat, lon) => (lat, lon)
        }).toList
      }
    }).headOption.
      getOrElse((new Exception("Failed to find 'polygon' item")).left)
  }

  val extractTownshipRangeSection = 
    selectTownshipRangeSection(_: RSSChannel) >>= parseTownshipRangeSection

  def getPart[A](parser: (RSSChannel) => \/[Throwable, A]):
      xml.Elem => \/[Throwable, A] =
    getTownshipGeoCoder(_: xml.Elem) >>= parser

  val getLatLon: (xml.Elem) => \/[Throwable, (Double, Double)] =
    getPart(extractLatLon)

  val getTownshipRangeSection: 
      (xml.Elem) => \/[Throwable, List[(Double, Double)]] =
    getPart(extractTownshipRangeSection)

  def requestLatLon(trs: TRS) =
    request(trs) map (_ >>= getLatLon)

  def requestTownshipRangeSection(trs: TRS) =
    request(trs) map (_ >>= getTownshipRangeSection)
}
