// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scala.language.higherKinds
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{ Failure => SFailure, Success => SSuccess }
import scala.xml
import scalaz._
import iteratee._
import iteratee.{ Iteratee => I }
import effect._
import concurrent._
import Scalaz._
import dispatch._

abstract class TownshipGeoCoder[F[_]] {

  implicit val tr: Traverse[F]

  import TownshipGeoCoder._

  val townshipGeoCoder = url(
    "http://www.geocommunicator.gov/TownshipGeoCoder/TownshipGeoCoder.asmx/GetLatLon")

  val georssNamespace = "http://www.georss.org/georss"

  implicit val ec: ExecutionContext

  def trsProps(trs: TRS): String = {
    List(
      trs.state.toString,
      trs.principalMeridian.id.shows,
      trs.townshipNumber.shows,
      Tag.unwrap(trs.townshipFraction).shows,
      trs.townshipDirection.toString,
      trs.rangeNumber.shows,
      Tag.unwrap(trs.rangeFraction).shows,
      trs.rangeDirection.toString,
      Tag.unwrap(trs.sectionNumber).shows,
      trs.sectionDivision.take(2).reverse.mkString(""),
      trs.townshipDuplicate.shows).mkString(",")
  }

  val requestTimeout = 2000 // ms

  protected lazy val http = {
    val result = Http.configure { builder =>
      builder.setRequestTimeout(requestTimeout)
    }
    // Shutting down the Http instance is required to prevent a hang on exit.
    Http.shutdown()
    result
  }

  // Unfortunately, due to a bug in Dispatch (or its dependencies), Http
  // instances need to be shut down to avoid a hang on program exit.
  def shutdown(): Unit = {
    http.shutdown()
  }

  protected def query(trs: TRS) =
    townshipGeoCoder <<? Map("TRS" -> trsProps(trs))

  def request(trs: TRS): Task[xml.Elem] = {
    val req = http(query(trs) OK as.xml.Elem)
    Task.async { cb =>
      req onComplete {
        case SSuccess(e) => cb(e.right)
        case SFailure(th) => cb(th.left)
      }
    }
  }

  def selectDataElem: (xml.Elem) => \/[Throwable, xml.Elem] = elem =>
  if (elem.label == "TownshipGeocoderResult")
    (\/.fromTryCatchNonFatal {
      (elem \ "CompletionStatus").text match {
        case "false" =>
          (new Exception((elem \ "Message").text)).left
        case "true" =>
          xml.XML.loadString((elem \ "Data").text).right
      }
    }).join
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
    node.label == "point" && node.namespace == georssNamespace
  } map { node =>
    \/.fromTryCatchNonFatal {
      node.text.split(' ').toList map (_.toDouble) match {
        case List(lon, lat) => (lat, lon)
      }
    }
  }).headOption.getOrElse((new Exception("Failed to find 'point' item")).left)

  def selectTownshipRangeSectionElem: (RSSChannel) => \/[Throwable, RSSItem] =
    selectGeoCoderElem("Township Range Section")

  def parseTownshipRangeSection:
      (RSSItem) => \/[Throwable, List[(Double, Double)]] = ch => {
    (ch.content filter { node =>
      node.label == "polygon" && node.namespace == georssNamespace
    } map { node =>
      \/.fromTryCatchNonFatal {
        (node.text.split(',').toList map (_.toDouble) grouped(2) map {
          case List(lat, lon) => (lat, lon)
        }).toList
      }
    }).headOption.
      getOrElse((new Exception("Failed to find 'polygon' item")).left)
  }

  def findPart[A](parser: (RSSChannel) => \/[Throwable, A]):
      xml.Elem => \/[Throwable, A] =
    (selectDataElem(_: xml.Elem) >>= parseRss >>=
      selectTownshipGeoCoderElem >>= parser)

  def findLatLon: (xml.Elem) => \/[Throwable, (Double, Double)] =
    findPart(selectLatLonElem(_: RSSChannel) >>= parseLatLon)

  def findTownshipRangeSection:
      (xml.Elem) => \/[Throwable, List[(Double, Double)]] =
    findPart(selectTownshipRangeSectionElem(_: RSSChannel) >>=
      parseTownshipRangeSection)

  def sendRequest: EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO]

  def projectLatLon: EnumerateeT[F[Requested], F[LatLonResponse], IO] =
    Iteratee.map { freq =>
      freq map { thtel =>
        thtel.fold(
          ths => Task.now(ths.left),
          tel => tel map { el =>
            findLatLon(el).leftMap(NonEmptyList(_))
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
  type Requested = ThrowablesOr[Task[xml.Elem]]
  type LatLonResponse = Task[ThrowablesOr[(Double, Double)]]

  def resource[F[_], G <: TownshipGeoCoder[F]]: Resource[G] =
    new Resource[G] {
      override def close(g: G): IO[Unit] = {
        IO(g.shutdown())
      }
    }
}

class MeteredTownshipGeoCoder[F[_]](
  implicit val tr: Traverse[F],
  implicit val ec: ExecutionContext)
    extends TownshipGeoCoder[F] {

  import TownshipGeoCoder._

  def makeRequestAfter(prev: BooleanLatch, fthtrs: F[ThrowablesOr[TRS]]):
      IO[(F[Requested], BooleanLatch)] =
    for {
      fReqNext <- (fthtrs.map { thtrs =>
        val io: IO[(Requested, BooleanLatch)] =
          thtrs.fold(
            ths => IO((-\/(ths), prev)),
            trs => {
              for {
                next <- IO(BooleanLatch())
                t <- IO(Task.fork {
                  for {
                    _ <- Task.now(prev.await())
                    req <- request(trs)
                    _ <- Task.now(next.release())
                  } yield req
                })
              } yield (\/-(t), next)
            })
        io
      }).sequenceU
    } yield {
      val fReq = fReqNext map (rn => rn._1)
      val next = fReqNext map (rn => rn._2) index(0) getOrElse prev
      (fReq, next)
    }

  def sendRequest: EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO] =
    new EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO] {
      def loop[A](prev: BooleanLatch) =
        step(prev).
          andThen(I.cont[F[ThrowablesOr[TRS]], IO, StepT[F[Requested], IO, A]])

      def step[A](prev: BooleanLatch):
          ((Input[F[Requested]] => IterateeT[F[Requested], IO, A]) =>
            Input[F[ThrowablesOr[TRS]]] =>
            IterateeT[F[ThrowablesOr[TRS]], IO, StepT[F[Requested], IO, A]]) =
        k => in => {
          in(
            el = fthtrs => {
              IterateeT.IterateeTMonadTrans.liftM(
                makeRequestAfter(prev, fthtrs)).flatMap {
                case (req, latch) =>
                  k(I.elInput(req)) >>== I.doneOr(loop(latch))
              }
            },
            empty = I.cont(step(prev)(k)),
            eof = I.done(I.scont(k), I.emptyInput))
        }

      override def apply[A] = {
        val latch = BooleanLatch()
        latch.release()
        I.doneOr(loop(latch))
      }
    }
}

class UnlimitedTownshipGeoCoder[F[_]](
  implicit val tr: Traverse[F],
  implicit val ec: ExecutionContext)
    extends TownshipGeoCoder[F] {

  import TownshipGeoCoder._

  def makeRequest(fthtrs: F[ThrowablesOr[TRS]]): IO[F[Requested]] = {
    val fio: F[IO[Requested]] =
      fthtrs.map { thtrs =>
        thtrs.fold(
          ths => IO(-\/(ths)),
          trs => IO(\/-(request(trs))))
      }
    fio.sequenceU
  }

  def oneReq: IterateeT[F[ThrowablesOr[TRS]], IO, F[Requested]] = {
    def step(s: Input[F[ThrowablesOr[TRS]]]):
        IterateeT[F[ThrowablesOr[TRS]], IO, F[Requested]] =
      s(el = fthtrs =>
        IterateeT.IterateeTMonadTrans.liftM(makeRequest(fthtrs)).
          flatMap (a => I.done(a, I.emptyInput)),
        empty = I.cont(step),
        eof = I.cont(step))
    I.cont(step)
  }

  def sendRequest: EnumerateeT[F[ThrowablesOr[TRS]], F[Requested], IO] =
    oneReq.sequenceI
}
