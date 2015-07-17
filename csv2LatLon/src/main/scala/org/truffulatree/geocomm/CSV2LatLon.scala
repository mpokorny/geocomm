package org.truffulatree.geocomm

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
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
          ch <- latLons(arg0).run
          _ <- showLatLons(ch)
        } yield ()
    } getOrElse {
      showUsage
    }
  }

  def showUsage: IO[Unit] =
    IO.putStrLn("Usage: csv2latlon [CSV input file path]")

  def latLons(filename: String):
      IterateeT[IoRecPlus[ThrowablesOr[TRS]], IO, Chan[Option[LatLonResult]]] = {
    val geocoder = new MeteredTownshipGeoCoder[IoRecPlus]
    FutureCompletion.collect[(Double,Double),IoRecPlus] { ioe =>
      ioe.fold(
        th => IO.putStrLn(s"ERROR: Failure reading input file: ${th.getMessage}"),
        _ => IO(()))
    } %=
    geocoder.requestLatLon &=
    trsRecords(filename)
  }

  def showLatLons(ch: Chan[Option[LatLonResult]]): IO[Unit] = {
    val el = for {
      optLlr <- ch.read
      _ <- optLlr.map { llr =>
        IO.putStrLn(llr.fold(_.getMessage, toLatLonCSV(_)))
      } getOrElse(IO(()))
    } yield optLlr
    el iterateWhile (_.isDefined) map (_ => ())
  }
}
