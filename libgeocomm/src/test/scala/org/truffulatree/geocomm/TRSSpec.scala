// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scalaz.Tag
import org.scalatest.PartialFunctionValues
import org.scalatest.exceptions.TestFailedException

class TRSSpec extends UnitSpec with PartialFunctionValues {

  "TRS Fraction type" should {
    "be represented by integers in {0,1,2,3}" in {
      Tag.unwrap(TRS.Fraction.valueAt(0)) should equal (0)
      Tag.unwrap(TRS.Fraction.valueAt(1)) should equal (1)
      Tag.unwrap(TRS.Fraction.valueAt(2)) should equal (2)
      Tag.unwrap(TRS.Fraction.valueAt(3)) should equal (3)
    }

    "have a minimum value of 0" in {
      TRS.FractionEnum.min should equal (Some(0))
    }

    "have a maximum value of 3" in {
      TRS.FractionEnum.max should equal (Some(3))
    }

    "not allow integers outside of set {0,1,2,3}" in {
      an[TestFailedException] should be thrownBy TRS.Fraction.valueAt(-1)
      an[TestFailedException] should be thrownBy TRS.Fraction.valueAt(4)
    }
  }

  "TRS Section type" should {
    val sectionMin = 1
    val sectionMax = 36

    "be represented by integers in range [1,2,...,36]" in {
      val range = sectionMin to sectionMax
      (range map (i => Tag.unwrap(TRS.Section.valueAt(i))) should contain 
        theSameElementsInOrderAs range)
    }

    "have a minimum value of 1" in {
      TRS.SectionEnum.min should equal (Some(sectionMin))
    }

    "have a maximum value of 36" in {
      TRS.SectionEnum.max should equal (Some(sectionMax))
    }

    "not allow integers outside of range [1,2,...,36]" in {
      (an[TestFailedException] should be thrownBy
        TRS.Section.valueAt(sectionMin - 1))

      (an[TestFailedException] should be thrownBy
        TRS.Section.valueAt(sectionMax + 1))
    }
  }

}
