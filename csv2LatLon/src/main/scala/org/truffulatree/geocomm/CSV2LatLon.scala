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

object Main extends SafeApp {
  override def runl(args: List[String]): IO[Unit] = {
    args.headOption.map { filename =>
      for {
        ss <- (Iteratee.collectT[IoLatLonResponse, IO, List] %=
          getLatLon %=
          getResponses %=
          sendRequest(5 second) %=
          parseRecords %=
          getRecords &=
          enumerateLines(new File(filename))).run
        ll <- checkResponses(ss)
        _ <- (headerLine(ll) ++ csvLines(ll)).map(IO.putStrLn(_)).sequenceU
      } yield ()
    } getOrElse {
      IO.putStrLn("ERROR: No file name provided")
    }
  }

  def showResponse(parsed: List[IoResponse]): IO[List[Unit]] = {
    ((parsed map { p =>
      p.fold(
        ex => ex.getMessage,
        par => par.toString
      )
    }) map (IO.putStrLn _)).sequenceU
  }

  def checkResponses(responses: List[IoLatLonResponse]): IO[List[LatLonResponse]] = {
    def printErr(bad: List[IoLatLonResponse]) =
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

  def headerLine(recs: List[LatLonResponse]): List[String] =
    recs.headOption.map(toHeader _).toList

  def csvLines(recs: List[LatLonResponse]): List[String] =
    recs map (toLatLonCSV _)
}
