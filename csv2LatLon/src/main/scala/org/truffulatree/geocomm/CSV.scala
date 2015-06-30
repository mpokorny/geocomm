package org.truffulatree.geocomm

import java.io.File
import scala.concurrent.{ Await, Future, TimeoutException }
import scala.concurrent.duration._
import scala.language.higherKinds
import scala.collection.SortedMap
import scala.xml
import scalaz._
import iteratee._
import Iteratee._
import effect._
import Scalaz._

object CSV {

  object Columns extends Enumeration {
    type Column = Value
    val State = Value("State")
    val PrincipalMeridian = Value("Principal Meridian")
    val TownshipNumber = Value("Township")
    val TownshipFraction = Value("Township Fraction")
    val TownshipDirection = Value("Township Direction")
    val RangeNumber = Value("Range")
    val RangeFraction = Value("Range Fraction")
    val RangeDirection = Value("Range Direction")
    val SectionNumber = Value("Section")
    val SectionDivision = Value("Section Division")
    val TownshipDuplicate = Value("Township Duplicate")
    val Latitude = Value("Latitude")
    val Longitude = Value("Longitude")
    val Comment = Value("Comment")
  }
  import Columns._

  type CSVRecord = SortedMap[String, String]

  def getState(record: CSVRecord): Validation[Throwable, States.State] =
    Validation.fromTryCatchNonFatal {
      States.withName(record.get(State.toString).getOrElse("NM"))
    }

  def getPrincipalMeridian(record: CSVRecord):
      Validation[Throwable, PrincipalMeridians.PM] =
    Validation.fromTryCatchNonFatal {
      PrincipalMeridians.
        withName(record.get(PrincipalMeridian.toString).
        getOrElse("NewMexico"))
    }

  def getTRNumber(record: CSVRecord, field: Column): 
      Validation[Throwable, Int] =
    Validation.fromTryCatchNonFatal(record(field.toString).toInt)

  def getTRSFraction(record: CSVRecord, field: Column):
      Validation[Throwable, TRS.Fractions.Fraction] =
    Validation.fromTryCatchNonFatal {
      TRS.Fractions(record.get(field.toString).map(_.toInt) getOrElse 0)
    }

  def getTownshipDirection(record: CSVRecord): 
      Validation[Throwable, Directions.NS] =
    Validation.fromTryCatchNonFatal {
      Directions.ns(record(TownshipDirection.toString))
    }

  def getRangeDirection(record: CSVRecord): 
      Validation[Throwable, Directions.EW] =
    Validation.fromTryCatchNonFatal {
      Directions.ew(record(RangeDirection.toString))
    }

  def getSectionNumber(record: CSVRecord): 
      Validation[Throwable, TRS.Sections.Section] =
    Validation.fromTryCatchNonFatal {
      TRS.Sections(record(SectionNumber.toString).toInt)
    }

  def getSectionDivision(record: CSVRecord):
      Validation[Throwable, List[Directions.Corner]] =
    Validation.fromTryCatchNonFatal {
      record(SectionDivision.toString).
        map(d => Directions.division(d.toString.toInt - 1)).reverse.toList
    }

  def getTownshipDuplicate(record: CSVRecord): Validation[Throwable, Int] =
    Validation.fromTryCatchNonFatal {
      record.get(TownshipDuplicate.toString).map(_.toInt) getOrElse 0
    }

  def convertRecord(record: CSVRecord) = (
    getState(record).toValidationNel |@|
      getPrincipalMeridian(record).toValidationNel |@|
      getTRNumber(record, TownshipNumber).toValidationNel |@|
      getTRSFraction(record, TownshipFraction).toValidationNel |@|
      getTownshipDirection(record).toValidationNel |@|
      getTRNumber(record, RangeNumber).toValidationNel |@|
      getTRSFraction(record, RangeFraction).toValidationNel |@|
      getRangeDirection(record).toValidationNel |@|
      getSectionNumber(record).toValidationNel |@|
      getSectionDivision(record).toValidationNel |@|
      getTownshipDuplicate(record).toValidationNel) {
    TRS(_,_,_,_,_,_,_,_,_,_,_)
  }

  def enumerateLines(file: File) = {
    lazy val it = scala.io.Source.fromFile(file).getLines
    enumIoSource(
      () => IoExceptionOr(it.hasNext),
      (b: IoExceptionOr[Boolean]) => b exists identity,
      (_: Boolean) => it.next
    )
  }

  type IoStr = IoExceptionOr[String]
  type IoRec = IoExceptionOr[(Int, Char, CSVRecord)]

  def getRecords: EnumerateeT[IoStr, IoRec, IO] =
    new EnumerateeT[IoStr, IoRec, IO] {
      def header(hdr: String): (Char, List[String]) = {
        val trimmedHdr = hdr.trim
        val sep = {
          val fields = trimmedHdr.split(Array(' ', '\t')) filter (_.length > 0)
          fields find (_.length == 1) map (_.head) getOrElse {
            if (fields.length > 1) {
              fields.map(_.last).
                find(Set(',', ';', ':', '|') contains (_)) getOrElse ','
            } else ';'
          }
        }
        (sep, trimmedHdr.split(sep).map(_.trim).toList)
      }
      def apply[A] = {
        def step0:
            ((StepT[IoRec, IO, A]) => 
              IterateeT[IoStr, IO, StepT[IoRec, IO, A]]) = stp => {
          stp(
            cont = in => {
              cont((iiostr: Input[IoStr]) => {
                iiostr(
                  el = iostr => {
                    iostr.fold(
                      exc => done(stp, Input(IoExceptionOr.ioException(exc))),
                      str => {
                        val (sep, cols) = header(str)
                        val ordering = Order.orderBy(cols.zipWithIndex.toMap)
                        loop(0, sep, cols, ordering)(in)
                      })
                  },
                  empty = step0(stp),
                  eof = done(stp, iiostr))
              })
            },
            done = (a, in) => {
              done(stp, eofInput)
            })
        }
        def loop(recNum: Int, sep: Char, cols: List[String], order: Order[String]) =
          step(recNum, sep, cols, order).andThen(
            cont[IoStr, IO, StepT[IoRec, IO, A]])
        def step(recNum: Int, sep: Char, cols: List[String], order: Order[String]):
            ((Input[IoRec] => IterateeT[IoRec, IO, A]) =>
              Input[IoStr] => IterateeT[IoStr, IO, StepT[IoRec, IO, A]]) = {
          k => in => {
            in(
              el = iostr => {
                iostr.fold(
                  exc => {
                    k(elInput(IoExceptionOr.ioException(exc))) >>==
                    doneOr(loop(recNum + 1, sep, cols, order))
                  },
                  str => {
                    k(elInput(IoExceptionOr(
                      (recNum,
                        sep,
                        SortedMap(
                          cols.zip(str.split(sep).map(_.trim)):_*)(
                          order.toScalaOrdering))))) >>==
                    doneOr(loop(recNum + 1, sep, cols, order))
                  })
              },
              empty = cont(step(recNum, sep, cols, order)(k)),
              eof = done(scont(k), in))
          }
        }
        step0
      }
    }

  type RecPlus[A] = (Int, Char, CSVRecord, A)

  type Parsed = RecPlus[ValidationNel[Throwable, TRS]]
  type IoParsed = IoExceptionOr[Parsed]

  def parseRecords: EnumerateeT[IoRec, IoParsed, IO] =
    Iteratee.map { iorec =>
      iorec map {
        case (recNum, sep, rec) =>
          (recNum, sep, rec, convertRecord(rec))
      }
    }

  type Requested =
    RecPlus[ValidationNel[Throwable, (Deadline, Future[xml.Elem])]]
  type IoRequested = IoExceptionOr[Requested]

  def sendRequest(requestTimeout: FiniteDuration):
      EnumerateeT[IoParsed, IoRequested, IO] =
    Iteratee.map { ioparsed =>
      ioparsed map {
        case (recNum, sep, rec, vtrs) =>
          (recNum, sep, rec, vtrs map (trs =>
            (Deadline.now + requestTimeout, TownshipGeocoder.request(trs))))
      }
    }

  type Response = RecPlus[\/[NonEmptyList[Throwable],xml.Elem]]
  type IoResponse = IoExceptionOr[Response]

  def completeRequest(req: Requested): Response = req match {
    case (recNum, sep, rec, vreq) =>
      (recNum,
        sep,
        rec,
        vreq.fold(
          ths => ths.left,
          fr => fr match {
            case (deadline, future) =>
              if (future.isCompleted) 
                Await.result(future, -1.seconds).right
              else
                NonEmptyList(
                  new TimeoutException("ERROR: Request timed out")).left
          }))
  }
 
  def partitionComplete(reqs: Vector[Requested]):
      (Vector[Response], Vector[Requested]) = {
    val (c, uc) = reqs partition {
      case (_, _, _, vreq) => 
        vreq.isFailure || vreq.exists {
          case (deadline, future) =>
            future.isCompleted || deadline.isOverdue
        }
    }
    (c map (completeRequest _), uc)
  }

  def getResponses: EnumerateeT[IoRequested, IoResponse, IO] =
    new EnumerateeT[IoRequested, IoResponse, IO] {
      def apply[A] = {
        def nextResponseOrCont(
          rsps: Vector[Response],
          reqs: Vector[Requested],
          k: (Input[IoResponse] => IterateeT[IoResponse, IO, A])):
            IterateeT[IoRequested, IO, StepT[IoResponse, IO, A]] = {
          val (rsps1, reqs1) = {
            val (newRsps, remReqs) = partitionComplete(reqs)
            (rsps ++ newRsps, remReqs)
          }
          rsps1.headOption map { rsp =>
            k(elInput(IoExceptionOr(rsp))) >>== doneOr(loop(rsps1.tail, reqs1))
          } getOrElse {
            cont(step(rsps1, reqs1)(k))
          }
        }

        def loop(rsps: Vector[Response], reqs: Vector[Requested]) =
          step(rsps, reqs) andThen cont[IoRequested, IO, StepT[IoResponse, IO, A]]

        def step(rsps: Vector[Response], reqs: Vector[Requested]):
            ((Input[IoResponse] => IterateeT[IoResponse, IO, A]) =>
              Input[IoRequested] =>
              IterateeT[IoRequested, IO, StepT[IoResponse, IO, A]]) = {
          k => in => {
            in(
              el = ioreq => {
                ioreq.fold(
                  exc => drainNextResponseOrContOrDone(rsps, reqs, exc.some, k, in),
                  req => nextResponseOrCont(rsps, reqs :+ req, k))
              },
              empty = nextResponseOrCont(rsps, reqs, k),
              eof = drainNextResponseOrContOrDone(rsps, reqs, none, k, in))
          }
        }

        def drainNextResponseOrContOrDone(
          rsps: Vector[Response],
          reqs: Vector[Requested],
          oex: Option[IoExceptionOr.IoException],
          k: Input[IoResponse] => IterateeT[IoResponse, IO, A],
          in: Input[IoRequested]): 
            IterateeT[IoRequested, IO, StepT[IoResponse, IO, A]] = {
          val (rsps1, reqs1) = {
            val (newRsps, remReqs) = partitionComplete(reqs)
            (rsps ++ newRsps, remReqs)
          }
          rsps1.headOption map { rsp =>
            k(elInput(IoExceptionOr(rsp))) >>==
            doneOr(drainLoop(rsps1.tail, reqs1, oex))
          } getOrElse {
            if (!reqs1.isEmpty) {
              cont(drainStep(rsps1, reqs1, oex)(k))
            }
            else {
              oex map { ex =>
                k(elInput(IoExceptionOr.ioException(ex))) >>==
                doneOr(drainLoop(Vector.empty, Vector.empty, none))
              } getOrElse {
                done[IoRequested, IO, StepT[IoResponse, IO, A]](
                  scont(k), in)
              }
            }
          }
        }

        def drainLoop(
          rsps: Vector[Response], 
          reqs: Vector[Requested], 
          ex: Option[IoExceptionOr.IoException]) =
          drainStep(rsps, reqs, ex).andThen(
            cont[IoRequested, IO, StepT[IoResponse, IO, A]])

        def drainStep(
          rsps: Vector[Response],
          reqs: Vector[Requested],
          ex: Option[IoExceptionOr.IoException]):
            ((Input[IoResponse] => IterateeT[IoResponse, IO, A]) =>
              Input[IoRequested] =>
              IterateeT[IoRequested, IO, StepT[IoResponse, IO, A]]) = {
          k => in => {
            in(
              el = _ => drainNextResponseOrContOrDone(rsps, reqs, ex, k, in),
              empty = drainNextResponseOrContOrDone(rsps, reqs, ex, k, in),
              eof = drainNextResponseOrContOrDone(rsps, reqs, ex, k, in))
          }
        }

        doneOr(loop(Vector.empty, Vector.empty))
      }
    }

  type LatLonResponse = RecPlus[\/[NonEmptyList[Throwable],(Double, Double)]]
  type IoLatLonResponse = IoExceptionOr[LatLonResponse]

  def getLatLon: EnumerateeT[IoResponse, IoLatLonResponse, IO] =
    Iteratee.map { iollrsp =>
      iollrsp map {
        case (recNum, sep, rec, vel) =>
          (recNum,
            sep,
            rec,
            vel flatMap (el =>
              TownshipGeocoder.getLatLon(el).leftMap(NonEmptyList(_))))
      }
    }

  def toLatLonCSV(recp: RecPlus[\/[NonEmptyList[Throwable], (Double, Double)]]):
      String = {
    val (_, sep, rec, va) = recp
    val newCols = va.fold(
      ths => List("", "", ths.map(_.getMessage).shows),
      a => List(a._1.shows, a._2.shows, ""))
    val newRec = rec.values.toList ++ newCols
    newRec.mkString(s"${sep.toString}")
  }

  def toHeader(recp: RecPlus[_]): String = {
    val (_, sep, rec, _) = recp
    (rec.keys.toList ++ List(Latitude, Longitude, Comment)).mkString(s"${sep.toString}")
  }
}
