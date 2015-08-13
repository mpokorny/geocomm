// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scala.util.{ Failure => SFailure, Success => SSuccess }
import scala.concurrent.ExecutionContext
import scalaz._
import concurrent.Task
import Scalaz._
import dispatch._

trait DispatchTRSRequester extends TRSRequester {

  implicit val ec: ExecutionContext

  lazy val llUrl = url(getLatLonURL)

  protected def query(trs: TRS) =
    llUrl <<? Map("TRS" -> trsProps(trs))

  protected lazy val http = {
    val requestTimeout = 2000 // ms
    val result = Http.configure { builder =>
      builder.setRequestTimeout(requestTimeout)
    }
    // Shutting down the Http instance is required to prevent a hang on exit.
    Http.shutdown()
    result
  }

  // Unfortunately, due to a bug in Dispatch (or its dependencies), Http
  // instances need to be shut down to avoid a hang on program exit.
  def shutdown(): Unit = {
    http.shutdown()
  }

  def request(trs: TRS): Task[xml.Elem] = {
    val req = http(query(trs) OK as.xml.Elem)
    Task.async { cb =>
      req onComplete {
        case SSuccess(e) => cb(e.right)
        case SFailure(th) => cb(th.left)
      }
    }
  }
}
