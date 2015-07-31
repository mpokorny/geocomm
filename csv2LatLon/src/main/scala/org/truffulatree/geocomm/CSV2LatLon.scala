package org.truffulatree.geocomm

import scala.concurrent.ExecutionContext.Implicits.global
import scalaz._
import effect._
import iteratee._
import concurrent.Chan
import Scalaz._
import CSV._

object Main extends SafeApp {

  type LatLonResult = RecPlus[ThrowablesOr[(Double, Double)]]
  type LatLonResponse = RecPlus[TownshipGeoCoder.LatLonResponse]

  implicit lazy val gcResource =
    TownshipGeoCoder.resource[RecPlus, MeteredTownshipGeoCoder[RecPlus]]

  override def runl(args: List[String]): IO[Unit] = {
    args.headOption map { arg0 =>
      if (arg0 == "-h" || arg0 == "--help" || args.length > 1)
        showUsage
      else
        IO(new MeteredTownshipGeoCoder[RecPlus]) using { geocoder =>
          for {
            ch <- latLons(geocoder, arg0).run
            _ <- (writeCSV(arg0 + ".output") &= ChanEnumerator(ch)).run
          } yield ()
        }
    } getOrElse {
      showUsage
    }
  }

  def showUsage: IO[Unit] =
    IO.putStrLn("Usage: csv2latlon [CSV input file path]")

  def latLons(geocoder: TownshipGeoCoder[RecPlus], filename: String):
      IterateeT[RecPlus[ThrowablesOr[TRS]], IO, Chan[Option[LatLonResult]]] = {
    TaskCompletion.collect[(Double,Double),RecPlus] %=
    geocoder.requestLatLon &=
    trsRecords(filename)
  }
}
