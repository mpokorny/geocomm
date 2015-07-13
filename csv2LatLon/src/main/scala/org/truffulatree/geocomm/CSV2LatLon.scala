package org.truffulatree.geocomm

import scala.language.postfixOps
import java.io.{ Writer, BufferedWriter, FileWriter }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._
import effect._
import iteratee._
import Iteratee._
import Scalaz._
import CSV._
import TownshipGeoCoder.LatLonResponse

object Main extends SafeApp {
  override def runl(args: List[String]): IO[Unit] = {
    args.headOption map { arg0 =>
      if (arg0 == "-h" || arg0 == "--help" || args.length > 1)
        showUsage
      else
        latLons(arg0).run
    } getOrElse {
      showUsage
    }
  }

  def showUsage: IO[Unit] = {
    IO.putStrLn("Usage: XXX [CSV file path]")
  }

  type LatLonPlus = RecPlus[LatLonResponse]
  type LatLonResult = IoExceptionOr[RecPlus[LatLonResponse]]

  def latLons(filename: String): IterateeT[_, IO, Unit] = {
    val geocoder =
      new MeteredTownshipGeoCoder[({type F[X] = IoExceptionOr[RecPlus[X]]})#F]
    writeCSV(filename + ".output") %=
    geocoder.requestLatLon(5 second) &=
    trsRecords(filename)
  }

  def writeCSV(filename: String): IterateeT[LatLonResult, IO, Unit] = {
    def openWriter: Input[LatLonResult] => IterateeT[LatLonResult, IO, Unit] =
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
        empty = cont(openWriter),
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
              th => IO.putStrLn(s"ERROR: Failed to write to output file: ${th.getMessage}").
                map(_ => none),
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
    cont(openWriter)
  }
}
