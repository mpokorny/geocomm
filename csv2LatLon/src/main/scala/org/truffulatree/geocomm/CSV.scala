package org.truffulatree.geocomm

import scala.language.higherKinds
import java.io.{ BufferedReader, FileReader, Writer, BufferedWriter, FileWriter }
import scala.collection.SortedMap
import scala.xml
import scala.concurrent.ExecutionContext.Implicits.global
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
      Validation[Throwable, PrincipalMeridians.PM] = {
    val pmCol = PrincipalMeridian.toString
    if (!record.contains(pmCol)) {
      Validation.fromTryCatchNonFatal {
        PrincipalMeridians.withName("NewMexico")
      }
    } else {
      Validation.fromTryCatchNonFatal {
        PrincipalMeridians(record(pmCol).toInt)
      } ||| Validation.fromTryCatchNonFatal {
        PrincipalMeridians.withName(record(pmCol))
      }
    }
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

  def convertRecord(record: CSVRecord): ValidationNel[Throwable, TRS] = (
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

  def enumerateLines(filename: String): EnumeratorT[IoExceptionOr[String], IO] = {
    def tryIO[A, B](action: => IoExceptionOr[B]) =
      IterateeT[A, IO, IoExceptionOr[B]](
        IO(action).map(r => sdone(r, r.fold(_ => eofInput, _ => emptyInput)))
      )

    def enum(r: => BufferedReader) =
      new EnumeratorT[IoExceptionOr[String], IO] {
        lazy val reader = r
        override def apply[A] = { (s: StepT[IoExceptionOr[String], IO, A]) =>
          s.mapCont { k =>
            tryIO(IoExceptionOr(reader.readLine())).flatMap {
              case IoExceptionOr(null) => s.pointI
              case ln @ IoExceptionOr(line) => k(elInput(ln)) >>== apply[A]
              case ioe => k(elInput(ioe))
            }
          }
        }
      }

    new EnumeratorT[IoExceptionOr[String], IO] {
      override def apply[A] = { (s: StepT[IoExceptionOr[String], IO, A]) =>
        s.mapCont { k =>
          tryIO(IoExceptionOr(new BufferedReader(new FileReader(filename)))).
            flatMap {
              case IoExceptionOr(reader) => IterateeT(
                enum(reader).apply(s).value.ensuring(IO(reader.close())))
              case ioe => k(elInput(ioe.map(_ => "")))
            }
        }
      }
    }
  }

  type IoStr = IoExceptionOr[String]
  type IoRec = IoExceptionOr[(Int, CSVRecord)]

  def getRecords: EnumerateeT[IoStr, IoRec, IO] =
    new EnumerateeT[IoStr, IoRec, IO] {

      def loop0[A] = step0 andThen cont[IoStr, IO, StepT[IoRec, IO, A]]

      def step0[A]: ((Input[IoRec] => IterateeT[IoRec, IO, A]) =>
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
                  val cols = str.split(",").toList
                  val ordering = Order.orderBy(cols.zipWithIndex.toMap)
                  cont(step(0, cols, ordering)(k))
                })
            },
            empty = cont(step0(k)),
            eof = done(scont(k), in))
        }

      def loop[A](recNum: Int, cols: List[String], order: Order[String]) =
        step(recNum, cols, order).andThen(cont[IoStr, IO, StepT[IoRec, IO, A]])

      def step[A](
        recNum: Int,
        cols: List[String],
        order: Order[String]):
          ((Input[IoRec] => IterateeT[IoRec, IO, A]) =>
            Input[IoStr] => IterateeT[IoStr, IO, StepT[IoRec, IO, A]]) = {
        k => in => {
          in(
            el = iostr => {
              iostr.fold(
                exc => {
                  k(elInput(IoExceptionOr.ioException(exc))) >>==
                  doneOr(loop(recNum + 1, cols, order))
                },
                str => {
                  k(elInput(IoExceptionOr(
                    (recNum,
                      SortedMap(
                        cols.zip(str.split(",")):_*)(
                        order.toScalaOrdering))))) >>==
                  doneOr(loop(recNum + 1, cols, order))
                })
            },
            empty = cont(step(recNum, cols, order)(k)),
            eof = done(scont(k), in))
        }
      }

      override def apply[A] = doneOr(loop0)
    }

  type RecPlus[A] = (Int, CSVRecord, A)
  type Parsed = RecPlus[ThrowablesOr[TRS]]
  type IoParsed = IoExceptionOr[Parsed]

  implicit object IoReqPlusFunctor
      extends Functor[({ type F[X] = IoExceptionOr[RecPlus[X]] })#F] {
    override def map[A, B](iora: IoExceptionOr[RecPlus[A]])(f: A => B):
        IoExceptionOr[RecPlus[B]] =
      iora.map { ra =>
        ra match {
          case (recNum, rec, a) => (recNum, rec, f(a))
        }
      }
  }

  implicit object IoReqPlusFoldable
      extends Foldable[({ type F[X] = IoExceptionOr[RecPlus[X]] })#F] {
    override def foldMap[A, B](iora: IoExceptionOr[RecPlus[A]])(f: A => B)(
      implicit F: Monoid[B]): B =
      iora.fold(
        _ => F.zero,
        ra => ra match {
          case (_, _, a) => f(a)
        })

    override def foldRight[A, B](iora: IoExceptionOr[RecPlus[A]], z: => B)(
      f: (A, => B) => B): B =
      iora.fold(
        _ => z,
        ra => ra match {
          case (_, _, a) => f(a, z)
        })
  }

  def parseRecords: EnumerateeT[IoRec, IoParsed, IO] =
    Iteratee.map { iorec =>
      iorec map {
        case (recNum, rec) => (recNum, rec, convertRecord(rec).disjunction)
      }
    }

  def trsRecords[A](filename: String): EnumeratorT[IoParsed, IO] =
    parseRecords.run(getRecords.run(enumerateLines(filename)))

  def toLatLonCSV(recp: RecPlus[TownshipGeoCoder.LatLonResponse]): String = {
    val (_, rec, va) = recp
    val newCols = va.fold(
      ths => List(
        "",
        "",
        s""""${ths.map(_.getMessage).shows.filterNot(_ == '"')}""""),
      a => List(a._1.shows, a._2.shows, ""))
    val newRec = rec.values.toList ++ newCols
    newRec.mkString(",")
  }

  def toHeader(recp: RecPlus[_]): String = {
    val (_, rec, _) = recp
    (rec.keys.toList ++ List(Latitude, Longitude, Comment)).mkString(s",")
  }

  type LatLonPlus = RecPlus[TownshipGeoCoder.LatLonResponse]
  type LatLonResult = IoExceptionOr[RecPlus[TownshipGeoCoder.LatLonResponse]]

  def startCSVOutput(filename: String):
      Input[LatLonResult] => IterateeT[LatLonResult, IO, Unit] =
    in => in(
      el = llr => IterateeT.IterateeTMonadTrans[LatLonResult].liftM {
        llr.fold[IO[Option[BufferedWriter]]](
          th => {
            IO.putStrLn(s"ERROR: Failure reading input file: ${th.getMessage}").
              map(_ => none)
          },
          _ => {
            IO(IoExceptionOr(new BufferedWriter(new FileWriter(filename)))) >>=
            { r =>
              r.fold(
                th => IO.putStrLn(s"ERROR: Failure opening output file: ${th.getMessage}").
                  map(_ => none),
                w => IO(w.some))
            }
          })
      } flatMap (ow =>
        ow.map(w => writeHeader(w, llr)).getOrElse(done((), eofInput))),
      empty = cont(startCSVOutput(filename)),
      eof = done((), eofInput))

  def writeHeader(writer: BufferedWriter, llr: LatLonResult):
      IterateeT[LatLonResult, IO, Unit] =
    IterateeT.IterateeTMonadTrans[LatLonResult].liftM {
      llr.fold[IO[Option[BufferedWriter]]](
        _ => IO(none),
        ll => IO(IoExceptionOr {
          writer.write(toHeader(ll))
          writer.newLine()
          writer.write(toLatLonCSV(ll))
          writer.newLine()
        }) >>= { r =>
          r.fold(
            th => IO.putStrLn(s"ERROR: Failed to write to output file: ${th.getMessage}") >>
              thenClose(writer),
            _ => IO(writer.some))
        })
    } flatMap (ow => writeRecords(ow))

  def thenClose(writer: BufferedWriter): IO[Option[BufferedWriter]] = {
    (IO(IoExceptionOr(writer.close())) >>= { r =>
      r.fold(
        th => IO.putStrLn(s"ERROR: Failed to close output file:${th.getMessage}"),
        _ => IO(()))
    }) >> IO(none[BufferedWriter])
  }

  def writeRecords(writer: Option[BufferedWriter]):
      IterateeT[LatLonResult, IO, Unit] =
    foldM[LatLonResult, IO, Option[BufferedWriter]](writer) { (ow, llr) =>
      llr.fold(
        th => {
          (OptionT(IO(ow)) flatMap { w =>
            OptionT {
              IO.putStrLn(s"ERROR: Failure reading input file: ${th.getMessage}") >>
              thenClose(w)
            }
          }).run
        },
        ll => {
          (OptionT(IO(ow)) flatMap { w =>
            OptionT {
              IO(IoExceptionOr {
                w.write(toLatLonCSV(ll))
                w.newLine()
              }) >>= { r =>
                r.fold(
                  th => {
                    IO.putStrLn(s"ERROR: Failed to write to output file: ${th.getMessage}") >>
                    thenClose(w)
                  },
                  _ => IO(ow))
              }
            }
          }).run
        })
    } flatMap { ow =>
      IterateeT.IterateeTMonadTrans[LatLonResult].liftM {
        ow.map(w => thenClose(w) >> IO(())).getOrElse(IO(()))
      }
    }

  def writeCSV(filename: String): IterateeT[LatLonResult, IO, Unit] =
    cont(startCSVOutput(filename))
}
