package org.truffulatree.geocomm

import scala.language.higherKinds
import java.util.Date
import scala.concurrent.{ Await, ExecutionContext, Future, TimeoutException }
import scala.concurrent.duration._
import scala.xml
import scalaz._
import iteratee._
import Iteratee._
import effect.{ IO, IoExceptionOr }
import Scalaz._
import dispatch._, Defaults._

abstract class TownshipGeoCoder[F[_]] {

  implicit val tr: Traverse[F]

  import TownshipGeoCoder._

  val townshipGeoCoder = url(
    "http://www.geocommunicator.gov/TownshipGeoCoder/TownshipGeoCoder.asmx/GetLatLon")

  implicit val ec: ExecutionContext

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

  protected def query(trs: TRS) =
    townshipGeoCoder <<? Map("TRS" -> trsProps(trs))

  def request(trs: TRS): IO[Future[xml.Elem]]

  def getDataElem: (xml.Elem) => \/[Throwable, xml.Elem] = elem =>
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

  def selectTownshipGeoCoderElem:
      (RSSChannel) => \/[Throwable, RSSChannel] = ch =>
  if (ch.title == "Township GeoCoder") ch.right
  else (new Exception("'Township GeoCoder' element not found")).left

  def selectGeoCoderElem(itemName: String):
      (RSSChannel) => \/[Throwable, RSSItem] =
    ch => {
      val item = ch.content filter (_.title == itemName)
      \/.fromEither(
        Either.cond(
          !item.isEmpty,
          item.head,
          new Exception(s"'$itemName' item not found")))
    }

  def selectLatLonElem: (RSSChannel) => \/[Throwable, RSSItem] =
    selectGeoCoderElem("Lat Lon")

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

  def getTownshipGeoCoderElem: (xml.Elem) => \/[Throwable, RSSChannel] =
    getDataElem(_: xml.Elem) >>= parseRss >>= selectTownshipGeoCoderElem

  def extractLatLon: (RSSChannel) => \/[Throwable, (Double, Double)] =
    selectLatLonElem(_: RSSChannel) >>= parseLatLon

  def selectTownshipRangeSectionElem: (RSSChannel) => \/[Throwable, RSSItem] =
    selectGeoCoderElem("Township Range Section")

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

  def extractTownshipRangeSection:
      (RSSChannel) => \/[Throwable, List[(Double, Double)]] =
    selectTownshipRangeSectionElem(_: RSSChannel) >>= parseTownshipRangeSection

  def getPart[A](parser: (RSSChannel) => \/[Throwable, A]):
      xml.Elem => \/[Throwable, A] =
    getTownshipGeoCoderElem(_: xml.Elem) >>= parser

  def getLatLon: (xml.Elem) => \/[Throwable, (Double, Double)] =
    getPart(extractLatLon)

  def getTownshipRangeSection:
      (xml.Elem) => \/[Throwable, List[(Double, Double)]] =
    getPart(extractTownshipRangeSection)

  def makeRequest(fthtrs: F[ThrowablesOr[TRS]]): IO[F[Requested]] =
    fthtrs.traverse { thtrs =>
      thtrs.fold(
        ths => IO(-\/(ths)),
        trs => request(trs) map (r => \/-(r)))
    }

  def itRequest(fthtrs: F[ThrowablesOr[TRS]]):
      IterateeT[F[ThrowablesOr[TRS]], IO, F[Requested]] =
    IterateeT {
      makeRequest(fthtrs).map(r => sdone(r, emptyInput))
    }

  def sendRequest: EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO] = {
    def inp: (
      Input[F[ThrowablesOr[TRS]]] =>
      IterateeT[F[ThrowablesOr[TRS]], IO, F[Requested]]) = { in =>
      in(
        el = fthtrs => itRequest(fthtrs),
        empty = cont(inp),
        eof = done(???, in))
    }
    IterateeT.cont(inp).sequenceI
  }

  def projectLatLon: EnumerateeT[F[Requested], F[LatLonResponse], IO] =
    Iteratee.map { freq =>
      freq map { thfel =>
        thfel.fold(
          ths => Future.successful(ths.left),
          fel => fel flatMap { el =>
            Future.successful(getLatLon(el).leftMap(NonEmptyList(_)))
          } recoverWith {
            case th: Exception =>
              Future.successful(NonEmptyList(th).left)
          })
      }
    }

  def requestLatLon: EnumerateeT[F[ThrowablesOr[TRS]], F[LatLonResponse], IO] =
    new EnumerateeT[F[ThrowablesOr[TRS]], F[LatLonResponse], IO] {
      override def apply[A] =
        k => projectLatLon.apply(k) %= sendRequest
    }
}

object TownshipGeoCoder {
  type Requested = ThrowablesOr[Future[xml.Elem]]
  //type Response = ThrowablesOr[xml.Elem]
  type LatLonResponse = Future[ThrowablesOr[(Double, Double)]]
}

class MeteredTownshipGeoCoder[F[_]](
  implicit val tr: Traverse[F], 
  val ec: ExecutionContext)
    extends TownshipGeoCoder[F] {

  var lastRequest = Future.successful[xml.Elem](<empty/>)

  def request(trs: TRS): IO[Future[xml.Elem]] = {
    // Slamming the geocoder web service as quickly as possible produces
    // unreliable results, so we limit ourselves to only one outstanding request
    // at a time. Whether that is the best policy is undetermined.
    lastRequest = lastRequest >> Http(query(trs) OK as.xml.Elem)
    IO(lastRequest)
  }
}

class UnlimitedTownshipGeoCoder[F[_]](
  implicit val tr: Traverse[F], 
  val ec: ExecutionContext)
    extends TownshipGeoCoder[F] {

  def request(trs: TRS): IO[Future[xml.Elem]] =
    IO(Http(query(trs) OK as.xml.Elem))
}
