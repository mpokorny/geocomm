package org.truffulatree.geocomm

import scala.language.higherKinds
import java.io.File
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
      () => IoExceptionOr(it.hasNext) ,
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
        def loop0 = step0 andThen cont[IoStr, IO, StepT[IoRec, IO, A]]
        def step0: ((Input[IoRec] => IterateeT[IoRec, IO, A]) =>
          Input[IoStr] => IterateeT[IoStr, IO, StepT[IoRec, IO, A]]) =
          k => in => {
            in(
              el = iostr => {
                iostr.fold(
                  exc => {
                    k(elInput(IoExceptionOr.ioException(exc))) >>==
                    doneOr(loop0)
                  },
                  str => {
                    val (sep, cols) = header(str)
                    val ordering = Order.orderBy(cols.zipWithIndex.toMap)
                    cont(step(0, sep, cols, ordering)(k))
                  })
              },
              empty = cont(step0(k)),
              eof = done(scont(k), in))
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
        doneOr(loop0)
      }
    }

  type RecPlus[A] = (Int, Char, CSVRecord, A)
  type Parsed = RecPlus[ThrowablesOr[TRS]]
  type IoParsed = IoExceptionOr[Parsed]

  implicit object IoReqPlusFunctor
      extends Functor[({ type F[X] = IoExceptionOr[RecPlus[X]] })#F] {
    def map[A, B](iora: IoExceptionOr[RecPlus[A]])(f: A => B): 
        IoExceptionOr[RecPlus[B]] =
      iora.map { ra =>
        ra match {
          case (recNum, sep, rec, a) =>
            (recNum, sep, rec, f(a))
        }
      }
  }

  implicit object IoReqPlusFoldable
      extends Foldable[({ type F[X] = IoExceptionOr[RecPlus[X]] })#F] {
    def foldMap[A, B](iora: IoExceptionOr[RecPlus[A]])(f: A => B)(
      implicit F: Monoid[B]): B =
      iora.fold(
        _ => F.zero,
        ra => ra match {
          case (_, _, _, a) => f(a)
        })

    def foldRight[A, B](iora: IoExceptionOr[RecPlus[A]], z: => B)(
      f: (A, => B) => B): B =
      iora.fold(
        _ => z,
        ra => ra match {
          case (_, _, _, a) => f(a, z)
        })
  }

  def parseRecords: EnumerateeT[IoRec, IoParsed, IO] =
    Iteratee.map { iorec =>
      iorec map {
        case (recNum, sep, rec) =>
          (recNum, sep, rec, convertRecord(rec).disjunction)
      }
    }

  def trsRecords[A, F[_]](filename: String): 
      IterateeT[IoParsed, IO, F[A]] => IterateeT[IoStr, IO, F[A]] = it => 
  (it %= parseRecords %= getRecords &= enumerateLines(new File(filename)))

  def toLatLonCSV(recp: RecPlus[TownshipGeoCoder.LatLonResponse]): String = {
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
