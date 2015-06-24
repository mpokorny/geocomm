package org.truffulatree.geocomm

import java.io.File
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.higherKinds
import scalaz._
import Scalaz._

object TRSFractions extends Enumeration {
  type F = Value
  val Zero, One, Two, Three = Value
}

object TRSSections extends Enumeration(1) {
  type S = Value
  val One, Two, Three, Four, Five, Six = Value
  val Seven, Eight, Nine, Ten, Eleven, Twelve = Value
  val Thirteen, Fourteen, Fifteen, Sixteen, Seventeen, Eighteen = Value
  val Nineteen, Twenty, TwentyOne, TwentyTwo, TwentyThree, TwentyFour = Value
  val TwentyFive, TwentySix, TwentySeven, TwentyEight, TwentyNine, Thirty = Value
  val ThirtyOne, ThirtyTwo, ThirtyThree, ThirtyFour, ThirtyFive, ThirtySix = Value
}

case class TRS(
  state: States.State, 
  principalMeridian: PrincipalMeridians.PM,
  townshipNumber: Int,
  townshipFraction: TRSFractions.F,
  townshipDirection: Directions.NS,
  rangeNumber: Int,
  rangeFraction: TRSFractions.F,
  rangeDirection: Directions.EW,
  sectionNumber: TRSSections.S,
  sectionDivision: List[Directions.Corner], // most to least significant
  townshipDuplicate: Int)

object TRS {

  object CSVColumns extends Enumeration {
    type C = Value
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

  type CSVRecord = Map[String, String]
  type CSVRecordPlus[_] = Map[String, ValidationNel[Throwable,_]]
  type CSVRecLatLon = CSVRecordPlus[(Double, Double)]

  def parseCSVRecords(stream: Stream[String]):
      (Char, Stream[(CSVRecord, ValidationNel[Throwable, TRS])]) = 
    stream match {
      case header +: records =>
        val separator = {
          val fields = header.trim.split(Array(' ', '\t')).filter(_.length > 0)
          if (fields.length > 1) {
            if (fields(1).length == 1) fields(1).head
            else fields(0).last
          } else ';'
        }
        val columnNames = header.split(separator).toList map (_.trim)
        (separator, records map convertRecord(separator, columnNames))
    }

  def getState(record: CSVRecord): Validation[Throwable, States.State] =
    Validation.fromTryCatchNonFatal {
      States.withName(record.get(CSVColumns.State.toString).getOrElse("NM"))
    }

  def getPrincipalMeridian(record: CSVRecord):
      Validation[Throwable, PrincipalMeridians.PM] =
    Validation.fromTryCatchNonFatal {
      PrincipalMeridians.
        withName(record.get(CSVColumns.PrincipalMeridian.toString).
        getOrElse("NewMexico"))
    }

  def getTRNumber(record: CSVRecord, field: CSVColumns.C): 
      Validation[Throwable, Int] =
    Validation.fromTryCatchNonFatal(record(field.toString).toInt)

  def getTRSFraction(record: CSVRecord, field: CSVColumns.C):
      Validation[Throwable, TRSFractions.F] =
    Validation.fromTryCatchNonFatal {
      TRSFractions(record.get(field.toString).map(_.toInt).getOrElse(0))
    }

  def getTownshipDirection(record: CSVRecord): 
      Validation[Throwable, Directions.NS] =
    Validation.fromTryCatchNonFatal {
      Directions.ns(record(CSVColumns.TownshipDirection.toString))
    }

  def getRangeDirection(record: CSVRecord): 
      Validation[Throwable, Directions.EW] =
    Validation.fromTryCatchNonFatal {
      Directions.ew(record(CSVColumns.RangeDirection.toString))
    }

  def getSectionNumber(record: CSVRecord): 
      Validation[Throwable, TRSSections.S] =
    Validation.fromTryCatchNonFatal {
      TRSSections(record(CSVColumns.SectionNumber.toString).toInt)
    }

  def getSectionDivision(record: CSVRecord):
      Validation[Throwable, List[Directions.Corner]] =
    Validation.fromTryCatchNonFatal {
      record(CSVColumns.SectionDivision.toString).
        map(d => Directions.division(d.toString.toInt - 1)).reverse.toList
    }

  def getTownshipDuplicate(record: CSVRecord): Validation[Throwable, Int] =
    Validation.fromTryCatchNonFatal {
      record.get(CSVColumns.TownshipDuplicate.toString).map(_.toInt).
        getOrElse(0)
    }

  def convertRecord(sep: Char, colNames: List[String]) = (str: String) => {
    val record = (str.split(sep) map (_.trim) zip colNames).toMap
    val trs = (
      getState(record).toValidationNel |@|
        getPrincipalMeridian(record).toValidationNel |@|
        getTRNumber(record, CSVColumns.TownshipNumber).toValidationNel |@|
        getTRSFraction(record, CSVColumns.TownshipFraction).toValidationNel |@|
        getTownshipDirection(record).toValidationNel |@|
        getTRNumber(record, CSVColumns.RangeNumber).toValidationNel |@|
        getTRSFraction(record, CSVColumns.RangeFraction).toValidationNel |@|
        getRangeDirection(record).toValidationNel |@|
        getSectionNumber(record).toValidationNel |@|
        getSectionDivision(record).toValidationNel |@|
        getTownshipDuplicate(record).toValidationNel) {
          TRS(_,_,_,_,_,_,_,_,_,_,_)
        }
    (record, trs)
  }

  def genRequest[A,M[_]](
    csv: CSVRecord,
    vn: ValidationNel[Throwable, TRS],
    toLatLon: (TRS) => M[\/[Throwable,A]])(implicit mm: Monad[M]):
      M[CSVRecordPlus[A]] = {
    val csvp: CSVRecordPlus[A] = csv map { case (k, v) => (k, v.success) }
    val latLon = CSVColumns.LatLon.toString
    vn.fold(
      th => mm.point(th.failure),
      trs => toLatLon(trs) map (_.validation.toValidationNel)).
      map (ll => csvp + (latLon -> ll))
  }

  def genRequestSeq[A,M[_]](
    toLatLon: (TRS) => M[\/[Throwable,A]])(implicit mm: Monad[M]):
      ((Stream[(CSVRecord, ValidationNel[Throwable, TRS])]) =>
        M[Stream[CSVRecordPlus[A]]]) =
    (strm: Stream[(CSVRecord, ValidationNel[Throwable, TRS])]) =>
    mm.sequence(
      strm map {
        case (csv, vn) => genRequest(csv, vn, toLatLon)
      })

  def convert(
    requests: (Char, Stream[(CSVRecord, ValidationNel[Throwable, TRS])]))(
    implicit ec: ExecutionContext): Future[(Char, Stream[CSVRecLatLon])] =
    requests match {
      case (sep, strm) =>
        val req = genRequestSeq(TownshipGeocoder.requestLatLon _)
        req(strm) map ((sep, _))
    }

  def convertCSVRecords(stream: Stream[String])(implicit ec: ExecutionContext):
      Future[(Char, Stream[CSVRecLatLon])] =
    convert(parseCSVRecords(stream))
  
  def convertCSVFile(file: File)(implicit ec: ExecutionContext):
      Validation[String, Future[(Char, Stream[CSVRecLatLon])]] =
    \/.fromTryCatchNonFatal {
      scala.io.Source.fromFile(file).getLines.toStream
    }.validation bimap(_.getMessage, convertCSVRecords _)

}
