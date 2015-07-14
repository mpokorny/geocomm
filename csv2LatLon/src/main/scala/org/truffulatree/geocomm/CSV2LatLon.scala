package org.truffulatree.geocomm

import scala.language.postfixOps
import scala.concurrent.duration._
import scalaz._
import effect._
import iteratee._
import Scalaz._
import CSV._

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
    IO.putStrLn("Usage: csv2latlon [CSV input file path]")
  }

  def latLons(filename: String): IterateeT[_, IO, Unit] = {
    val geocoder =
      new MeteredTownshipGeoCoder[({type F[X] = IoExceptionOr[RecPlus[X]]})#F]
    writeCSV(filename + ".output") %=
    geocoder.requestLatLon(5 second) &=
    trsRecords(filename)
  }
}
