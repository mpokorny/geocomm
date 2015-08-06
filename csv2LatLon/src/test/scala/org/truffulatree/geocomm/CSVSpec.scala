package org.truffulatree.geocomm

import scala.collection.immutable.SortedMap
import scala.math.Ordering
import scalaz._
import Scalaz._

class CSVSpec extends UnitSpec {
  import CSV.Columns

  val basicMap =
    Map(
      Columns.State.toString -> "NM",
      Columns.PrincipalMeridian.toString -> "NewMexico",
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

  "CSV parsing" should {
    "convert 'NM' as the 'State' to New Mexico" in {
      CSV.getState(basicRecord) should equal (Success(States.NewMexico))
    }

    "default to NM for the 'State' field value" in {
      (CSV.getState(basicRecord - Columns.State.toString) should equal
        (Success(States.NewMexico)))
    }

    "convert 'NewMexico' as the 'Principal Meridian' to New Mexico PM" in {
      (CSV.getPrincipalMeridian(basicRecord) should
        equal (Success(PrincipalMeridians.NewMexico)))
    }

    "convert '23' as the 'Principal Meridian' to NewMexico PM" in {
      (CSV.getPrincipalMeridian(basicRecord + ("Principal Meridian" -> "23"))
        should equal (Success(PrincipalMeridians.NewMexico)))
    }

    "default to New Mexico PM for the 'Principal Meridian' field value" in {
      (CSV.getState(basicRecord - Columns.PrincipalMeridian.toString)
        should equal (Success(States.NewMexico)))
    }

    "accept values 0-3 for the 'Township Fraction' field" in {
      val range = 0 to 3
      ((range map (
        i => CSV.getTRSFraction(
          basicRecord + (Columns.TownshipFraction.toString -> i.toString),
          CSV.Columns.TownshipFraction)))
        should contain theSameElementsInOrderAs
        (range map (i => Success(TRS.Fraction(i)))))
    }

    "default to 0 for the 'Township Fraction' field" in {
      CSV.getTRSFraction(
        basicRecord - Columns.TownshipFraction.toString,
        CSV.Columns.TownshipFraction) should equal (Success(TRS.Fraction(0)))
    }
  }
}
