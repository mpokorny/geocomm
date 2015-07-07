package org.truffulatree.geocomm

import scala.language.postfixOps
import java.io.File
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._
import effect._
import iteratee._
import Scalaz._
import CSV._
import TownshipGeoCoder.LatLonResponse

object Main extends SafeApp {
  override def runl(args: List[String]): IO[Unit] = {
    args.headOption map { filename =>
      for {
        ss <- latLons(filename).run
        ll <- checkResponses(ss)
        _ <- (headerLine(ll) ++ csvLines(ll)).map(IO.putStrLn(_)).sequenceU
      } yield ()
    } getOrElse {
      IO.putStrLn("ERROR: No file name provided")
    }
  }

  type LatLonResult = IoExceptionOr[RecPlus[LatLonResponse]]

  def latLons(filename: String): IterateeT[_, IO, List[LatLonResult]] = {
    val geocoder =
      new MeteredTownshipGeoCoder[({type F[X] = IoExceptionOr[RecPlus[X]]})#F]
    trsRecords(filename)(
      geocoder.requestLatLon(5 second)(
        Iteratee.collectT[LatLonResult, IO, List]))
  }

  def checkResponses(responses: List[LatLonResult]):
      IO[List[RecPlus[LatLonResponse]]] = {
    def printErr(bad: List[LatLonResult]) =
      bad.headOption map { ioll =>
        ioll.fold(
          ex => IO.putStrLn(s"ERROR: Failed reading input file, output file may be incomplete: ${ex.getMessage}"),
          ll => IO(()))
      } getOrElse {
        IO(())
      }
    val (good, bad) = responses partition (_.toOption.isDefined)
    for {
      _ <- printErr(bad)
      g <- IO(good)
    } yield (g map (_.toOption.get))
  }

  def headerLine(recs: List[RecPlus[LatLonResponse]]): List[String] =
    recs.headOption.map(toHeader _).toList

  def csvLines(recs: List[RecPlus[LatLonResponse]]): List[String] =
    recs map (toLatLonCSV _)
}
