// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scalaz._
import concurrent.Task
import Scalaz._

trait TRSRequester {

  val getLatLonURL: String =
    "http://www.geocommunicator.gov/TownshipGeoCoder/TownshipGeoCoder.asmx/GetLatLon"

  def trsProps(trs: TRS): String = {
    List(
      trs.state.toString,
      trs.principalMeridian.id.shows,
      trs.townshipNumber.shows,
      Tag.unwrap(trs.townshipFraction).shows,
      trs.townshipDirection.toString,
      trs.rangeNumber.shows,
      Tag.unwrap(trs.rangeFraction).shows,
      trs.rangeDirection.toString,
      Tag.unwrap(trs.sectionNumber).shows,
      trs.sectionDivision.take(2).reverse.mkString(""),
      trs.townshipDuplicate.shows).mkString(",")
  }

  def shutdown(): Unit

  def request(trs: TRS): Task[xml.Elem]  
}
