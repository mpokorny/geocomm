package org.truffulatree.geocomm

import java.io.File
import scala.concurrent.{ ExecutionContext, Future }
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
    val State = Value("state")
    val PrincipalMeridian = Value("pm")
    val TownshipNumber = Value("township")
    val TownshipFraction = Value("townshipFraction")
    val TownshipDirection = Value("townshipDirection")
    val RangeNumber = Value("rangeNumber")
    val RangeFraction = Value("rangeFraction")
    val RangeDirection = Value("rangeDirection")
    val SectionNumber = Value("sectionNumber")
    val SectionDivision = Value("sectionDivision")
    val TownshipDuplicate = Value("townshipDuplicate")
    val LatLon = Value("latLon")
  }
  import Columns._

  type CSVRecord = SortedMap[String, String]
  type CSVRecordPlus[_] = SortedMap[String, ValidationNel[Throwable,_]]
  type CSVRecLatLon = CSVRecordPlus[(Double, Double)]

  // def parseCSVRecords(stream: Stream[String]):
  //     (Char, Stream[(CSVRecord, ValidationNel[Throwable, TRS])]) = 
  //   stream match {
  //     case header +: records =>
  //       val separator = {
  //         val fields = header.trim.split(Array(' ', '\t')).filter(_.length > 0)
  //         if (fields.length > 1) {
  //           if (fields(1).length == 1) fields(1).head
  //           else fields(0).last
  //         } else ';'
  //       }
  //       val columnNames = header.split(separator).toList map (_.trim)
  //       (separator, records map convertRecord(separator, columnNames))
  //   }

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

  // def convertRecord(sep: Char, colNames: List[String]) = (str: String) => {
  //   val record = (str.split(sep) map (_.trim) zip colNames).toMap
  //   val trs = (
  //     getState(record).toValidationNel |@|
  //       getPrincipalMeridian(record).toValidationNel |@|
  //       getTRNumber(record, TownshipNumber).toValidationNel |@|
  //       getTRSFraction(record, TownshipFraction).toValidationNel |@|
  //       getTownshipDirection(record).toValidationNel |@|
  //       getTRNumber(record, RangeNumber).toValidationNel |@|
  //       getTRSFraction(record, RangeFraction).toValidationNel |@|
  //       getRangeDirection(record).toValidationNel |@|
  //       getSectionNumber(record).toValidationNel |@|
  //       getSectionDivision(record).toValidationNel |@|
  //       getTownshipDuplicate(record).toValidationNel) {
  //         TRS(_,_,_,_,_,_,_,_,_,_,_)
  //       }
  //   (record, trs)
  // }

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

  // def genRequest[A,M[_]](
  //   csv: CSVRecord,
  //   vn: ValidationNel[Throwable, TRS],
  //   toLatLon: (TRS) => M[\/[Throwable,A]])(implicit mm: Monad[M]):
  //     M[CSVRecordPlus[A]] = {
  //   val csvp: CSVRecordPlus[A] = csv map { case (k, v) => (k, v.success) }
  //   val latLon = LatLon.toString
  //   vn.fold(
  //     th => mm.point(th.failure),
  //     trs => toLatLon(trs) map (_.validation.toValidationNel)).
  //     map (ll => csvp + (latLon -> ll))
  // }

  // def genRequestSeq[A,M[_]](
  //   toLatLon: (TRS) => M[\/[Throwable,A]])(implicit mm: Monad[M]):
  //     ((Stream[(CSVRecord, ValidationNel[Throwable, TRS])]) =>
  //       M[Stream[CSVRecordPlus[A]]]) =
  //   (strm: Stream[(CSVRecord, ValidationNel[Throwable, TRS])]) =>
  //   mm.sequence(
  //     strm map {
  //       case (csv, vn) => genRequest(csv, vn, toLatLon)
  //     })

  // def convert(
  //   requests: (Char, Stream[(CSVRecord, ValidationNel[Throwable, TRS])]))(
  //   implicit ec: ExecutionContext): Future[(Char, Stream[CSVRecLatLon])] =
  //   requests match {
  //     case (sep, strm) =>
  //       val req = genRequestSeq(TownshipGeocoder.requestLatLon _)
  //       req(strm) map ((sep, _))
  //   }

  // def convertCSVRecords(stream: Stream[String])(implicit ec: ExecutionContext):
  //     Future[(Char, Stream[CSVRecLatLon])] =
  //   ??? //convert(parseCSVRecords(stream))

  // def convertCSVFile(file: File)(implicit ec: ExecutionContext):
  //     Validation[String, Future[(Char, Stream[CSVRecLatLon])]] =
  //   \/.fromTryCatchNonFatal {
  //     scala.io.Source.fromFile(file).getLines.toStream
  //   }.validation bimap(_.getMessage, convertCSVRecords _)

  def enumerateLines(file: File) = {
    lazy val it = scala.io.Source.fromFile(file).getLines
    enumIoSource(
      () => IoExceptionOr(it.hasNext),
      (b: IoExceptionOr[Boolean]) => b exists identity,
      (_: Boolean) => it.next
    )
  }

  type IoStr = IoExceptionOr[String]
  type IoRec = IoExceptionOr[(Char, CSVRecord)]

  def getRecords: EnumerateeT[IoStr, IoRec, IO] =
    new EnumerateeT[IoStr, IoRec, IO] {
      def header(hdr: String): (Char, List[String]) = {
        val trimmedHdr = hdr.trim
        val sep = {
          val fields = trimmedHdr.split(Array(' ', '\t')) filter (_.length > 0)
          if (fields.length > 1) {
            if (fields(1).length == 1) fields(1).head
            else fields(0).last
          } else ';'
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
                        loop(sep, cols, ordering)(in)
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
        def loop(sep: Char, cols: List[String], order: Order[String]) =
          step(sep, cols, order) andThen cont[IoStr, IO, StepT[IoRec, IO, A]]
        def step(sep: Char, cols: List[String], order: Order[String]):
            ((Input[IoRec] => IterateeT[IoRec, IO, A]) =>
              Input[IoStr] => IterateeT[IoStr, IO, StepT[IoRec, IO, A]]) = {
          k => in => {
            in(
              el = iostr => {
                iostr.fold(
                  exc => done(scont(k), Input(IoExceptionOr.ioException(exc))),
                  str => {
                    k(elInput(IoExceptionOr(
                      (sep,
                        SortedMap(
                          cols.zip(str.split(sep).map(_.trim)):_*)(
                          order.toScalaOrdering))))) >>==
                    doneOr(loop(sep, cols, order))
                  })
              },
              empty = cont(step(sep, cols, order)(k)),
              eof = done(scont(k), in))
          }
        }
        step0
      }
    }

  type Parsed = (Char, CSVRecord, ValidationNel[Throwable, TRS])
  type IoParsed = IoExceptionOr[Parsed]

  def parseRecords: EnumerateeT[IoRec, IoParsed, IO] =
    Iteratee.map { iorec =>
      iorec map {
        case (sep, rec) =>
          (sep, rec, convertRecord(rec))
      }
    }

  object IoExceptionOrList extends Applicative[IoExceptionOr] {
    def ap[A, B](fa: => IoExceptionOr[A])(f: => IoExceptionOr[A => B]) =
      fa flatMap (a => f map (_(a)))
    def point[A](a: => A) =
      IoExceptionOr(a)
  }

  def parseFile(file: File): IO[IoExceptionOr[List[Parsed]]] =
    (collectT[IoParsed, IO, List] %= parseRecords %= getRecords &=
      enumerateLines(file)).run.map(IoExceptionOrList.sequence(_))

  def makeRequests(parsed: List[Parsed])(implicit ec: ExecutionContext):
      IO[List[Future[(Char, CSVRecord, ValidationNel[Throwable,xml.Elem])]]] =
    parsed.map {
      case (sep, rec, vtrs) =>
        vtrs.fold(
          ths => IO(Future.successful(
            (sep, rec,
              Validation.failure[NonEmptyList[Throwable],xml.Elem](ths)))),
          el => TownshipGeocoder.request(el) map (
            fx => fx map (x => ((sep, rec, Validation.success[NonEmptyList[Throwable],xml.Elem](x))))))
    }.sequenceU

  def combineResponses(
    fs: List[Future[(Char, CSVRecord, ValidationNel[Throwable, xml.Elem])]])(
    implicit ec: ExecutionContext):
      Future[List[(Char, CSVRecord, ValidationNel[Throwable, xml.Elem])]] =
    Future.sequence(fs)
}
