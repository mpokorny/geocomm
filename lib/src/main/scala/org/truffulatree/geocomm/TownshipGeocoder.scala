package org.truffulatree.geocomm

import scala.language.higherKinds
import java.util.Date
import scala.concurrent.{ Await, Future, TimeoutException }
import scala.concurrent.duration._
import scala.xml
import scalaz._
import iteratee._
import Iteratee._
import effect.{ IO, IoExceptionOr }
import Scalaz._
import dispatch._, Defaults._

abstract class TownshipGeoCoder[F[_]] {

  implicit val fn: Functor[F]

  implicit val fl: Foldable[F]

  import TownshipGeoCoder._

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

  def sendRequest(requestTimeout: FiniteDuration):
      EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO] =
    Iteratee.map { fthtrs =>
      fthtrs map { thtrs =>
        thtrs map (trs => (requestTimeout.fromNow, request(trs)))
      }
    }

  def completeRequest: F[Requested] => F[Response] = req => {
    req map { thdf =>
      thdf flatMap {
        case (_, future) =>
          if (future.isCompleted)
            Await.result(future, -1.seconds).right
          else
            NonEmptyList(new TimeoutException("ERROR: Request timed out")).left
      }
    }
  }

  def checkComplete: F[Requested] => Boolean =
    freq => {
      implicit object BooleanMonoid extends Monoid[Boolean] {
        def append(f1: Boolean, f2: => Boolean) = f1 && f2
        def zero = true
      }
      implicitly[Foldable[F]].foldMap(freq) { req =>
        req.isLeft || req.exists { r =>
          r match {
            case (deadline, future) =>
              future.isCompleted || deadline.isOverdue
          }
        }
      }
    }

  def partitionComplete(reqs: Vector[F[Requested]]):
      (Vector[F[Response]], Vector[F[Requested]]) = {
    val (c, uc) = reqs partition checkComplete
    (c map completeRequest, uc)
  }

  def getResponses: EnumerateeT[F[Requested], F[Response], IO] =
    new EnumerateeT[F[Requested], F[Response], IO] {

      def nextResponseOrCont[A](
        rsps: Vector[F[Response]],
        reqs: Vector[F[Requested]],
        k: Input[F[Response]] => IterateeT[F[Response], IO, A],
        in: Input[F[Requested]]):
          IterateeT[F[Requested], IO, StepT[F[Response], IO, A]] = {
        val (rsps1, reqs1) = {
          val (newRsps, remReqs) = partitionComplete(reqs)
          (rsps ++ newRsps, remReqs)
        }
        rsps1.headOption map { rsp =>
          k(elInput(rsp)) >>== doneOr(loop(rsps1.tail, reqs1))
        } getOrElse {
          k(emptyInput) >>== doneOr(k1 => cont(step(rsps1, reqs1)(k1)))
        }
      }

      def loop[A](rsps: Vector[F[Response]], reqs: Vector[F[Requested]]):
          ((Input[F[Response]] => IterateeT[F[Response], IO, A]) =>
            IterateeT[F[Requested], IO, StepT[F[Response], IO, A]]) =
        step(rsps, reqs).andThen(
          cont[F[Requested], IO, StepT[F[Response], IO, A]])

      def step[A](rsps: Vector[F[Response]], reqs: Vector[F[Requested]]):
          ((Input[F[Response]] => IterateeT[F[Response], IO, A]) =>
            Input[F[Requested]] =>
            IterateeT[F[Requested], IO, StepT[F[Response], IO, A]]) = {
        k => in => {
          in(
            el = freq => nextResponseOrCont(rsps, reqs :+ freq, k, in),
            empty = nextResponseOrCont(rsps, reqs, k, in),
            eof = drainResponses(rsps, reqs, in)(k))
        }
      }

      def drainResponses[A](
        rsps: Vector[F[Response]],
        reqs: Vector[F[Requested]],
        in: Input[F[Requested]]):
          ((Input[F[Response]] => IterateeT[F[Response], IO, A]) =>
            IterateeT[F[Requested], IO, StepT[F[Response], IO, A]]) =
        k => {
          val (rsps1, reqs1) = {
            val (newRsps, remReqs) = partitionComplete(reqs)
            (rsps ++ newRsps, remReqs)
          }
          rsps1.headOption map { rsp =>
            k(elInput(rsp)) >>== doneOr(drainResponses(rsps1.tail, reqs1, in))
          } getOrElse {
            if (!(rsps1.isEmpty && reqs1.isEmpty))
              k(emptyInput) >>== doneOr(drainResponses(rsps1, reqs1, in))
            else
              k(eofInput) >>== doneOr(k1 => done(scont(k1), in))
          }
        }

      def apply[A] =
        doneOr(loop(Vector.empty, Vector.empty))
    }

  def projectLatLon: EnumerateeT[F[Response], F[LatLonResponse], IO] =
    Iteratee.map { frsp =>
      frsp map { threl =>
        threl flatMap { el =>
          getLatLon(el).leftMap(NonEmptyList(_))
        }
      }
    }

  def requestLatLon[A](requestTimeout: FiniteDuration):
      (IterateeT[F[LatLonResponse], IO, A] =>
        IterateeT[F[ThrowablesOr[TRS]], IO, A]) = it =>
  (it %= projectLatLon %= getResponses %= sendRequest(requestTimeout))
}

object TownshipGeoCoder {
  type Requested = ThrowablesOr[(Deadline, Future[xml.Elem])]
  type Response = ThrowablesOr[xml.Elem]
  type LatLonResponse = ThrowablesOr[(Double, Double)]
}

class MeteredTownshipGeoCoder[F[_]](
  implicit val fn: Functor[F],
  val fl: Foldable[F])
    extends TownshipGeoCoder[F] {

  var lastRequest = Future.successful[xml.Elem](<empty/>)

  def request(trs: TRS): Future[xml.Elem] = {
    // Slamming the geocoder web service as quickly as possible produces
    // unreliable results, so we limit ourselves to only one outstanding request
    // at a time. Whether that is that best policy is undetermined.
    lastRequest = lastRequest >> Http(query(trs) OK as.xml.Elem)
    lastRequest
  }
}

class UnlimitedTownshipGeoCoder[F[_]](
  implicit val fn: Functor[F],
  val fl: Foldable[F])
    extends TownshipGeoCoder[F] {

  def request(trs: TRS): Future[xml.Elem] =
    Http(query(trs) OK as.xml.Elem)
}
