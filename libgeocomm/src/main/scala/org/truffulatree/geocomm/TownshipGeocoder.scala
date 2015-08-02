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
      trs.sectionDivision.take(2).reverse.mkString(""),
      trs.townshipDuplicate.shows).mkString(",")
  }

  protected lazy val http = {
    val result = Http.configure { builder => builder.setRequestTimeout(2000) }
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
      freq map { thtel =>
        thtel.fold(
          ths => Task.now(ths.left),
          tel => tel map { el =>
            getLatLon(el).leftMap(NonEmptyList(_))
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

  def resource[F[_], G <: TownshipGeoCoder[F]] =
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
      rn <- (fthtrs.map { thtrs =>
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
    } yield (rn map (_._1), rn map (_._2) index(0) getOrElse prev)

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
              IterateeT.IterateeTMonadTrans.liftM(makeRequestAfter(prev, fthtrs)).
                flatMap {
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
