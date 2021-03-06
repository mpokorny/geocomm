// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm.nmbg

import org.truffulatree.geocomm._
import CSV._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scalaz._
import effect._
import iteratee._
import concurrent.Chan
import Scalaz._

object Main extends SafeApp {

  type LatLonResult = RecPlus[ThrowablesOr[(Double, Double)]]
  type LatLonResponse = RecPlus[TownshipGeoCoder.LatLonResponse]

  class TGC(implicit val tr: Traverse[RecPlus], val ec: ExecutionContext)
      extends MeteredTownshipGeoCoder[RecPlus]
      with DispatchTRSRequester

  implicit lazy val tgcResource =
    TownshipGeoCoder.resource[RecPlus, TGC]

  override def runl(args: List[String]): IO[Unit] = {
    args.headOption map { arg0 =>
      if (arg0 == "-h" || arg0 == "--help" || args.length > 1)
        showUsage
      else
        IO(new TGC) using { geocoder =>
          for {
            ch <- latLons(geocoder, arg0).run
            _ <- (writeFile(arg0 + ".output") &= ChanEnumerator(ch)).run
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
    CSV.trsRecords(filename)
  }
}
