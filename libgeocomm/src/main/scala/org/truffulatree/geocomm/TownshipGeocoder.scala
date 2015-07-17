package org.truffulatree.geocomm

import scala.language.higherKinds
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import java.util.Date
import scala.xml
import scalaz._
import iteratee._
import iteratee.{ Iteratee => I }
import effect.{ IO, IoExceptionOr }
import Scalaz._
import dispatch._

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

  protected lazy val http = Http.configure { builder =>
    builder.setRequestTimeout(2000)
  }

  protected def query(trs: TRS) =
    townshipGeoCoder <<? Map("TRS" -> trsProps(trs))

  def request(trs: TRS): Future[xml.Elem] =
    http(query(trs) OK as.xml.Elem)

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

  def sendRequest: EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO]

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
  type LatLonResponse = Future[ThrowablesOr[(Double, Double)]]
}

class MeteredTownshipGeoCoder[F[_]](
  implicit val tr: Traverse[F], 
  implicit val ec: ExecutionContext)
    extends TownshipGeoCoder[F] {

  import TownshipGeoCoder._

  def makeRequestAfter(prev: Future[xml.Elem], fthtrs: F[ThrowablesOr[TRS]]):
      F[Requested] =
    fthtrs.map { thtrs =>
      thtrs.fold(
        ths => -\/(ths),
        trs => \/-(prev >> request(trs)))
    }

  def sendRequest: EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO] =
    new EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO] {
      def loop[A](prev: Future[xml.Elem]) =
        step(prev).
          andThen(I.cont[F[ThrowablesOr[TRS]], IO, StepT[F[Requested], IO, A]])

      def step[A](prev: Future[xml.Elem]):
          ((Input[F[Requested]] => IterateeT[F[Requested], IO, A]) =>
            Input[F[ThrowablesOr[TRS]]] =>
            IterateeT[F[ThrowablesOr[TRS]], IO, StepT[F[Requested], IO, A]]) =
        k => in => {
          in(
            el = fthtrs => {
              val req = makeRequestAfter(prev, fthtrs)
              val reqf = req.index(0).map(_.fold(
                _ => prev,
                identity)).getOrElse(prev)
              k(I.elInput(req)) >>== I.doneOr(loop(reqf))
            },
            empty = I.cont(step(prev)(k)),
            eof = I.done(I.scont(k), I.emptyInput))
        }

      override def apply[A] =
        I.doneOr(loop(Future.successful(<empty/>)))
    }
}


class UnlimitedTownshipGeoCoder[F[_]](
  implicit val tr: Traverse[F], 
  implicit val ec: ExecutionContext)
    extends TownshipGeoCoder[F] {

  import TownshipGeoCoder._

  def makeRequest(fthtrs: F[ThrowablesOr[TRS]]): F[Requested] =
    fthtrs.map { thtrs =>
      thtrs.fold(
        ths => -\/(ths),
        trs => \/-(request(trs)))
    }

  def sendRequest: EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO] =
    I.map(makeRequest(_))
}
