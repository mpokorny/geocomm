package org.truffulatree.geocomm

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import scalaz._
import effect._
import iteratee._
import concurrent.Chan
import Scalaz._
import CSV._

object Main extends SafeApp {

  type IoRecPlus[A] = IoExceptionOr[RecPlus[A]]
  type LatLonResult = IoRecPlus[ThrowablesOr[(Double, Double)]]
  type LatLonResponse = IoRecPlus[TownshipGeoCoder.LatLonResponse]

  override def runl(args: List[String]): IO[Unit] = {
    args.headOption map { arg0 =>
      if (arg0 == "-h" || arg0 == "--help" || args.length > 1)
        showUsage
      else
        for {
          llrs <- latLons(arg0).run
          ch <- Chan.newChan[LatLonResult]
          _ <- IO(llrs map writeToChan(ch))
          _ <- List.fill(llrs.length)(showLatLons(ch)).sequenceU
        } yield ()
    } getOrElse {
      showUsage
    }
  }

  def showUsage: IO[Unit] =
    IO.putStrLn("Usage: csv2latlon [CSV input file path]")

  def latLons(filename: String): IterateeT[_, IO, List[LatLonResponse]] = {
    val geocoder = new MeteredTownshipGeoCoder[IoRecPlus]
    Iteratee.collectT[LatLonResponse, IO, List] %=
    geocoder.requestLatLon &=
    trsRecords(filename)
  }

  def writeToChan(chan: Chan[LatLonResult])(implicit ec: ExecutionContext):
      LatLonResponse => Unit = { llr =>
    llr.fold(
      th => chan.write(IoExceptionOr.ioException(th)).unsafePerformIO,
      rpf => rpf match {
        case (recNum, rec, fll) => fll onComplete {
          case Success(ll) => 
            chan.write(IoExceptionOr.ioExceptionOr(
              (recNum, rec, ll))).unsafePerformIO
          case Failure(th) =>
            chan.write(IoExceptionOr.ioExceptionOr(
              (recNum, rec, NonEmptyList(th).left))).unsafePerformIO
        }
      })
  }

  def showLatLons(ch: Chan[LatLonResult]): IO[Unit] = {
    ch.read >>= { llr =>
      IO.putStrLn(llr.fold(_.getMessage, toLatLonCSV(_)))
    }
  }
}
