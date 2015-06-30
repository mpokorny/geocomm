package org.truffulatree.geocomm

import java.util.Date
import scala.concurrent.{ ExecutionContext, Future }
import scala.xml
import scalaz._
import Scalaz._
import effect.IO
import effect.IoExceptionOr
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

  def request(trs: TRS): Future[xml.Elem] = {
    val query = townshipGeocoder <<? Map("TRS" -> trsProps(trs))
    Http(query OK as.xml.Elem)
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
    RSS.parse _

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

  // type Requested =
  //   RecPlus[ValidationNel[Throwable, (Deadline, Future[xml.Elem])]]
  // type IoRequested = IoExceptionOr[Requested]

  // def sendRequest(requestTimeout: FiniteDuration):
  //     EnumerateeT[IoParsed, IoRequested, IO] =
  //   Iteratee.map { ioparsed =>
  //     ioparsed map {
  //       case (recNum, sep, rec, vtrs) =>
  //         (recNum, sep, rec, vtrs map (trs =>
  //           (Deadline.now + requestTimeout, TownshipGeocoder.request(trs))))
  //     }
  //   }

  // type Response = RecPlus[\/[NonEmptyList[Throwable],xml.Elem]]
  // type IoResponse = IoExceptionOr[Response]

  // def completeRequest(req: Requested): Response = req match {
  //   case (recNum, sep, rec, vreq) =>
  //     (recNum,
  //       sep,
  //       rec,
  //       vreq.fold(
  //         ths => ths.left,
  //         fr => fr match {
  //           case (deadline, future) =>
  //             if (future.isCompleted) 
  //               Await.result(future, -1.seconds).right
  //             else
  //               NonEmptyList(
  //                 new TimeoutException("ERROR: Request timed out")).left
  //         }))
  // }
 
  // def partitionComplete(reqs: Vector[Requested]):
  //     (Vector[Response], Vector[Requested]) = {
  //   val (c, uc) = reqs partition {
  //     case (_, _, _, vreq) => 
  //       vreq.isFailure || vreq.exists {
  //         case (deadline, future) =>
  //           future.isCompleted || deadline.isOverdue
  //       }
  //   }
  //   (c map (completeRequest _), uc)
  // }

  // def getResponses: EnumerateeT[IoRequested, IoResponse, IO] =
  //   new EnumerateeT[IoRequested, IoResponse, IO] {
  //     def apply[A] = {
  //       def nextResponseOrCont(
  //         rsps: Vector[Response],
  //         reqs: Vector[Requested],
  //         k: (Input[IoResponse] => IterateeT[IoResponse, IO, A])):
  //           IterateeT[IoRequested, IO, StepT[IoResponse, IO, A]] = {
  //         val (rsps1, reqs1) = {
  //           val (newRsps, allReqs) = partitionComplete(reqs)
  //           (rsps ++ newRsps, allReqs)
  //         }
  //         rsps1.headOption map { rsp =>
  //           k(elInput(IoExceptionOr(rsp))) >>== doneOr(loop(rsps1.tail, reqs1))
  //         } getOrElse {
  //           cont(step(rsps1, reqs1)(k))
  //         }
  //       }

  //       def loop(rsps: Vector[Response], reqs: Vector[Requested]) =
  //         step(rsps, reqs) andThen cont[IoRequested, IO, StepT[IoResponse, IO, A]]

  //       def step(rsps: Vector[Response], reqs: Vector[Requested]):
  //           ((Input[IoResponse] => IterateeT[IoResponse, IO, A]) =>
  //             Input[IoRequested] =>
  //             IterateeT[IoRequested, IO, StepT[IoResponse, IO, A]]) = {
  //         k => in => {
  //           in(
  //             el = ioreq => {
  //               ioreq.fold(
  //                 exc => drainNextResponseOrContOrDone(rsps, reqs, exc.some, k, in),
  //                 req => nextResponseOrCont(rsps, reqs :+ req, k))
  //             },
  //             empty = nextResponseOrCont(rsps, reqs, k),
  //             eof = drainNextResponseOrContOrDone(rsps, reqs, none, k, in))
  //         }
  //       }

  //       def drainNextResponseOrContOrDone(
  //         rsps: Vector[Response],
  //         reqs: Vector[Requested],
  //         oex: Option[IoExceptionOr.IoException],
  //         k: Input[IoResponse] => IterateeT[IoResponse, IO, A],
  //         in: Input[IoRequested]): 
  //           IterateeT[IoRequested, IO, StepT[IoResponse, IO, A]] = {
  //         val (rsps1, reqs1) = {
  //           val (newRsps, allReqs) = partitionComplete(reqs)
  //           (rsps ++ newRsps, allReqs)
  //         }
  //         rsps1.headOption map { rsp =>
  //           k(elInput(IoExceptionOr(rsp))) >>==
  //           doneOr(drainLoop(rsps1.tail, reqs1, oex))
  //         } getOrElse {
  //           if (!reqs.isEmpty) {
  //             cont(drainStep(rsps1, reqs1, oex)(k))
  //           }
  //           else {
  //             oex map { ex =>
  //               done[IoRequested, IO, StepT[IoResponse, IO, A]](
  //                 scont(k), Input(IoExceptionOr.ioException(ex)))
  //             } getOrElse {
  //               done[IoRequested, IO, StepT[IoResponse, IO, A]](
  //                 scont(k), in)
  //             }
  //           }
  //         }
  //       }

  //       def drainLoop(
  //         rsps: Vector[Response], 
  //         reqs: Vector[Requested], 
  //         ex: Option[IoExceptionOr.IoException]) =
  //         drainStep(rsps, reqs, ex).andThen(
  //           cont[IoRequested, IO, StepT[IoResponse, IO, A]])

  //       def drainStep(
  //         rsps: Vector[Response],
  //         reqs: Vector[Requested],
  //         ex: Option[IoExceptionOr.IoException]):
  //           ((Input[IoResponse] => IterateeT[IoResponse, IO, A]) =>
  //             Input[IoRequested] =>
  //             IterateeT[IoRequested, IO, StepT[IoResponse, IO, A]]) = {
  //         k => in => {
  //           in(
  //             el = _ => drainNextResponseOrContOrDone(rsps, reqs, ex, k, in),
  //             empty = drainNextResponseOrContOrDone(rsps, reqs, ex, k, in),
  //             eof = drainNextResponseOrContOrDone(rsps, reqs, ex, k, in))
  //         }
  //       }

  //       doneOr(loop(Vector.empty, Vector.empty))
  //     }
  //   }

  // type LatLonResponse = RecPlus[\/[NonEmptyList[Throwable],(Double, Double)]]
  // type IoLatLonResponse = IoExceptionOr[LatLonResponse]

  // def getLatLon: EnumerateeT[IoResponse, IoLatLonResponse, IO] =
  //   Iteratee.map { iollrsp =>
  //     iollrsp map {
  //       case (recNum, sep, rec, vel) =>
  //         (recNum,
  //           sep,
  //           rec,
  //           vel flatMap (el =>
  //             TownshipGeocoder.getLatLon(el).leftMap(NonEmptyList(_))))
  //     }
  //   }

}
