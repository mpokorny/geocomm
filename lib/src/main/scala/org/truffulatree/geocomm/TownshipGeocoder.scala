package org.truffulatree.geocomm

import java.util.Date
import scala.concurrent.Future
import scala.xml
import scalaz._
import Scalaz._
import effect.IO
import effect.IoExceptionOr
import dispatch._, Defaults._

abstract class TownshipGeoCoder {

  val townshipGeoCoder = url(
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

  def query(trs: TRS) = townshipGeoCoder <<? Map("TRS" -> trsProps(trs))

  def request(trs: TRS): Future[xml.Elem]

  def getData: (xml.Elem) => \/[Throwable, xml.Elem] = elem =>
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

  def parseRss: (xml.Elem) => \/[Throwable, RSSChannel] =
    RSS.parse _

  def selectTownshipGeoCoder: (RSSChannel) => \/[Throwable, RSSChannel] = ch =>
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

  def selectLatLon: (RSSChannel) => \/[Throwable, RSSItem] =
    selectGeoCoderItem("Lat Lon")

  def parseLatLon: (RSSItem) => \/[Throwable, (Double, Double)] = ch =>
    (ch.content filter { node =>
      node.label == "point" && node.namespace == "http://www.georss.org/georss"
    } map { node =>
      \/.fromTryCatchNonFatal {
        node.text.split(' ').toList map (_.toDouble) match {
          case List(lon, lat) => (lat, lon)
        }
      }
    }).headOption.getOrElse((new Exception("Failed to find 'point' item")).left)

  def getTownshipGeoCoder =
    getData(_: xml.Elem) >>= parseRss >>= selectTownshipGeoCoder

  def extractLatLon = selectLatLon(_: RSSChannel) >>= parseLatLon

  def selectTownshipRangeSection: (RSSChannel) => \/[Throwable, RSSItem] =
    selectGeoCoderItem("Township Range Section")

  def parseTownshipRangeSection:
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

  def extractTownshipRangeSection =
    selectTownshipRangeSection(_: RSSChannel) >>= parseTownshipRangeSection

  def getPart[A](parser: (RSSChannel) => \/[Throwable, A]):
      xml.Elem => \/[Throwable, A] =
    getTownshipGeoCoder(_: xml.Elem) >>= parser

  def getLatLon: (xml.Elem) => \/[Throwable, (Double, Double)] =
    getPart(extractLatLon)

  def getTownshipRangeSection: 
      (xml.Elem) => \/[Throwable, List[(Double, Double)]] =
    getPart(extractTownshipRangeSection)
}

class MeteredTownshipGeoCoder extends TownshipGeoCoder {

  var lastRequest = Future.successful[xml.Elem](<empty/>)

  def request(trs: TRS): Future[xml.Elem] = {
    // Slamming the geocoder web service as quickly as possible produces
    // unreliable results, so we limit ourselves to only one outstanding request
    // at a time. Whether that is that best policy is undetermined.
    lastRequest = lastRequest >> Http(query(trs) OK as.xml.Elem)
    lastRequest
  }
}

class UnlimitedTownshipGeoCoder extends TownshipGeoCoder {

  def request(trs: TRS): Future[xml.Elem] =
    Http(query(trs) OK as.xml.Elem)
}
