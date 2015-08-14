// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm.nmbg

import org.truffulatree.geocomm._
import scala.collection.immutable.SortedMap
import scala.math.Ordering
import scalaz._
import Scalaz._

class CSVSpec extends UnitSpec {
  import CSV.Columns

  val basicMap =
    Map(
      Columns.State.toString -> "NM",
      Columns.PrincipalMeridian.toString -> "New Mexico",
      Columns.TownshipNumber.toString -> "10",
      Columns.TownshipFraction.toString -> "0",
      Columns.TownshipDirection.toString -> "N",
      Columns.RangeNumber.toString -> "14",
      Columns.RangeFraction.toString -> "0",
      Columns.RangeDirection.toString -> "E",
      Columns.SectionNumber.toString -> "27",
      Columns.SectionDivision.toString -> "32",
      Columns.TownshipDuplicate.toString -> "0")

  val basicRecord =
    SortedMap(basicMap.toSeq:_*)(Ordering.by(basicMap.keys.zipWithIndex.toMap))

  val nmPM = TRS.PrincipalMeridian.apply(23)

  "CSV parsing" should {
    "convert 'NM' as the 'State' to New Mexico" in {
      CSV.convertState(basicRecord) should equal (Success(States.NM))
    }

    "default to NM for the 'State' field value" in {
      (CSV.convertState(basicRecord - Columns.State.toString) should equal
        (Success(States.NM)))
    }

    "convert 'NewMexico' as the 'Principal Meridian' to New Mexico PM" in {
      CSV.convertPrincipalMeridian(basicRecord) should equal (Success(nmPM))
    }

    "convert '23' as the 'Principal Meridian' to NewMexico PM" in {
      (CSV.convertPrincipalMeridian(
        basicRecord + ("Principal Meridian" -> "23"))
        should equal (Success(nmPM)))
    }

    "default to New Mexico PM for the 'Principal Meridian' field value" in {
      (CSV.convertPrincipalMeridian(
        basicRecord - Columns.PrincipalMeridian.toString)
        should equal (Success(nmPM)))
    }

    "accept values 0-3 for the 'Township Fraction' field" in {
      val range = 0 to 3
      ((range map (
        i => CSV.convertTRSFraction(
          basicRecord + (Columns.TownshipFraction.toString -> i.toString),
          CSV.Columns.TownshipFraction)))
        should contain theSameElementsInOrderAs
        (range map (i => Success(TRS.Fraction(i)))))
    }

    "default to 0 for the 'Township Fraction' field" in {
      CSV.convertTRSFraction(
        basicRecord - Columns.TownshipFraction.toString,
        CSV.Columns.TownshipFraction) should equal (Success(TRS.Fraction(0)))
    }
  }
}
