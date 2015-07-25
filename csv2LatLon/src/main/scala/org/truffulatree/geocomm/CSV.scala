package org.truffulatree.geocomm

import scala.language.higherKinds
import java.io.{ BufferedReader, FileReader, Writer, BufferedWriter, FileWriter }
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

  def enumerateLines(filename: String): EnumeratorT[String, IO] = {
    def tryIO[A, B](action: => IoExceptionOr[B]) =
      IterateeT[A, IO, IoExceptionOr[B]](
        IO(action).map(r => sdone(r, r.fold(_ => eofInput, _ => emptyInput)))
      )

    def enum(r: => BufferedReader) =
      new EnumeratorT[String, IO] {
        lazy val reader = r
        override def apply[A] = { (s: StepT[String, IO, A]) =>
          s.mapCont { k =>
            tryIO(IoExceptionOr(reader.readLine())).flatMap {
              case IoExceptionOr(null) => s.pointI
              case IoExceptionOr(line) => k(elInput(line)) >>== apply[A]
              case ioe @ _ => {
                IterateeT.IterateeTMonadTrans[String] liftM {
                  ioe.fold(
                    th => IO.putStrLn(s"Failure reading input file: ${th.getMessage}"),
                    _ => IO(())
                  )
                } flatMap (_ => s.pointI)
              }
            }
          }
        }
      }

    new EnumeratorT[String, IO] {
      override def apply[A] = { (s: StepT[String, IO, A]) =>
        s.mapCont { k =>
          tryIO(IoExceptionOr(new BufferedReader(new FileReader(filename)))).
            flatMap {
              case IoExceptionOr(reader) => IterateeT(
                enum(reader).apply(s).value.ensuring(IO(reader.close())))
              case ioe @ _ => {
                IterateeT.IterateeTMonadTrans[String] liftM {
                  ioe.fold(
                    th => IO.putStrLn(s"Failed to open input file: ${th.getMessage}"),
                    _ => IO(())
                  )
                } flatMap (_ => s.pointI)
              }
            }
        }
      }
    }
  }

  type Rec = (Int, CSVRecord)

  def getRecords: EnumerateeT[String, Rec, IO] =
    new EnumerateeT[String, Rec, IO] {

      def loop0[A] = step0 andThen cont[String, IO, StepT[Rec, IO, A]]

      def step0[A]: ((Input[Rec] => IterateeT[Rec, IO, A]) =>
        Input[String] => IterateeT[String, IO, StepT[Rec, IO, A]]) =
        k => in => {
          in(
            el = str => {
              val cols = str.split(",").toList
              val ordering = Order.orderBy(cols.zipWithIndex.toMap)
              cont(step(0, cols, ordering)(k))
            },
            empty = cont(step0(k)),
            eof = done(scont(k), emptyInput))
        }

      def loop[A](recNum: Int, cols: List[String], order: Order[String]) =
        step(recNum, cols, order).andThen(cont[String, IO, StepT[Rec, IO, A]])

      def step[A](
        recNum: Int,
        cols: List[String],
        order: Order[String]):
          ((Input[Rec] => IterateeT[Rec, IO, A]) =>
            Input[String] => IterateeT[String, IO, StepT[Rec, IO, A]]) = {
        k => in => {
          in(
            el = str => {
              k(elInput(
                (recNum,
                  SortedMap(
                    cols.zip(str.split(",")):_*)(
                    order.toScalaOrdering)))) >>==
              doneOr(loop(recNum + 1, cols, order))
            },
            empty = cont(step(recNum, cols, order)(k)),
            eof = done(scont(k), emptyInput))
        }
      }

      override def apply[A] = doneOr(loop0)
    }

  type RecPlus[A] = (Int, CSVRecord, A)
  type Parsed = RecPlus[ThrowablesOr[TRS]]

  implicit object ReqPlusTraverse1
      extends Traverse1[({ type F[X] = RecPlus[X] })#F] {
    override def traverse1Impl[G[_], A, B](ra: RecPlus[A])(f: (A) => G[B])(
      implicit arg0: Apply[G]): G[RecPlus[B]] =
      ra match {
        case (recNum, rec, a) =>
          f(a) map (b => (recNum, rec, b))
      }

    override def foldMapRight1[A, B](ra: RecPlus[A])(z: (A) ⇒ B)(
      f: (A, ⇒ B) ⇒ B): B = {
      ra match {
        case (recNum, rec, a) =>
          f(a, z(a))
      }
    }
  }

  def parseRecords: EnumerateeT[Rec, Parsed, IO] =
    Iteratee.map {
      case (recNum, rec) => (recNum, rec, convertRecord(rec).disjunction)
    }

  def trsRecords[A](filename: String): EnumeratorT[Parsed, IO] =
    parseRecords.run(getRecords.run(enumerateLines(filename)))

  def toLatLonCSV(recp: RecPlus[ThrowablesOr[(Double,Double)]]): String = {
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

  type LatLonPlus = RecPlus[ThrowablesOr[(Double,Double)]]

  def startCSVOutput(filename: String):
      Input[LatLonPlus] => IterateeT[LatLonPlus, IO, Unit] =
    in => in(
      el = llp => IterateeT.IterateeTMonadTrans[LatLonPlus].liftM {
        IO(IoExceptionOr(new BufferedWriter(new FileWriter(filename)))) >>=
        { r =>
          r.fold(
            th => IO.putStrLn(s"ERROR: Failure opening output file: ${th.getMessage}").
              map(_ => none[BufferedWriter]),
            w => IO(w.some))
        }
      } flatMap (ow =>
        ow.map(w => writeHeader(w, llp)).getOrElse(done((), eofInput))),
      empty = cont(startCSVOutput(filename)),
      eof = done((), eofInput))

  def writeHeader(writer: BufferedWriter, llp: LatLonPlus):
      IterateeT[LatLonPlus, IO, Unit] =
    IterateeT.IterateeTMonadTrans[LatLonPlus].liftM {
      IO(IoExceptionOr {
        val header = toHeader(llp)
        writer.write(header)
        writer.newLine()
        val rec = toLatLonCSV(llp)
        writer.write(rec)
        writer.newLine()
      }) >>= { r =>
        r.fold(
          th => IO.putStrLn(s"ERROR: Failed to write to output file: ${th.getMessage}") >>
            thenClose(writer),
          _ => IO(writer.some))
      }
    } flatMap (ow => writeRecords(ow))

  def thenClose(writer: BufferedWriter): IO[Option[BufferedWriter]] = {
    (IO(IoExceptionOr(writer.close())) >>= { r =>
      r.fold(
        th => IO.putStrLn(s"ERROR: Failed to close output file:${th.getMessage}"),
        _ => IO(()))
    }) >> IO(none[BufferedWriter])
  }

  def writeRecords(writer: Option[BufferedWriter]):
      IterateeT[LatLonPlus, IO, Unit] =
    foldM[LatLonPlus, IO, Option[BufferedWriter]](writer) { (ow, llp) =>
      (OptionT(IO(ow)) flatMap { w =>
        OptionT {
          IO(IoExceptionOr {
            val rec = toLatLonCSV(llp)
            w.write(rec)
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
    } flatMap { ow =>
      IterateeT.IterateeTMonadTrans[LatLonPlus].liftM {
        ow.map(w => thenClose(w) >> IO(())).getOrElse(IO(()))
      }
    }

  def writeCSV(filename: String): IterateeT[LatLonPlus, IO, Unit] =
    cont(startCSVOutput(filename))
}
