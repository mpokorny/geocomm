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
      an [TestFailedException] should be thrownBy TRS.Fraction.valueAt(-1)
      an [TestFailedException] should be thrownBy TRS.Fraction.valueAt(4)
    }
  }

  "TRS Section type" should {
    "be represented by integers in range [1,2,...,36]" in {
      val range = 1 to 36
      (range map (i => Tag.unwrap(TRS.Section.valueAt(i))) should contain 
        theSameElementsInOrderAs range)
    }

    "have a minimum value of 1" in {
      TRS.SectionEnum.min should equal (Some(1))
    }

    "have a maximum value of 36" in {
      TRS.SectionEnum.max should equal (Some(36))
    }

    "not allow integers outside of range [1,2,...,36]" in {
      an [TestFailedException] should be thrownBy TRS.Section.valueAt(0)
      an [TestFailedException] should be thrownBy TRS.Section.valueAt(37)
    }
  }

}
