// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scalaz._
import Scalaz._

case class TRS(
  state: States.State,
  principalMeridian: TRS.PrincipalMeridian,
  townshipNumber: Int,
  townshipFraction: TRS.Fraction,
  townshipDirection: Directions.NS,
  rangeNumber: Int,
  rangeFraction: TRS.Fraction,
  rangeDirection: Directions.EW,
  sectionNumber: TRS.Section,
  sectionDivision: List[Directions.Corner], // most to least significant
  townshipDuplicate: Int)

object TRS {

  sealed trait FractionT

  type Fraction = Int @@ FractionT

  def Fraction: PartialFunction[Int, Fraction] = {
    case i @ _ if FractionEnum.minVal <= i && i <= FractionEnum.maxVal =>
      Tag[Int, FractionT](i)
  }

  implicit object FractionEnum extends IntRangeEnum[FractionT] {
    override val minVal = 0
    override val maxVal = 3
    override val tagged = Fraction
  }


  sealed trait SectionT

  type Section = Int @@ SectionT

  def Section: PartialFunction[Int, Section] = {
    case (i) if SectionEnum.minVal <= i && i <= SectionEnum.maxVal =>
      Tag[Int, SectionT](i)
  }

  implicit object SectionEnum extends IntRangeEnum[SectionT] {
    override val minVal = 1
    override val maxVal = 36
    override val tagged = Section
  }

  sealed trait PrincipalMeridianT

  type PrincipalMeridian = Int @@ PrincipalMeridianT

  object PrincipalMeridian {
    def apply: PartialFunction[Int, PrincipalMeridian] = {
      case i @ _ if PrincipalMeridianEnum.minVal <= i &&
          i <= PrincipalMeridianEnum.maxVal =>
        Tag[Int, PrincipalMeridianT](i)
    }

    def lookup(str: String): Option[PrincipalMeridian] =
      names.get(str.toLowerCase)

    lazy val names = {
      val lcNames = List(
        "first", "second", "third", "fourth", "fifth", "sixth",
        "black hills", "boise", "chickasaw", "choctaw", "cimarron",
        "copper river", "fairbanks", "gila and salt river", "humboldt",
        "hunstville", "indian", "louisiana", "michigan", "montana",
        "mount diablo", "navajo", "new mexico", "st helens", "st stephens",
        "salt lake", "san bernardino", "seward", "tallahassee",
        "uintah special", "ute", "washington", "willamette", "wind river",
        "ohio", "INVALID", "muskigum river", "ohio river", "first scioto river",
        "second scioto river", "third scioto river", "ellicotts line",
        "twelve mile square", "kateel river", "umiat")
      (lcNames.zipWithIndex.toMap - "INVALID") mapValues (i => apply(i + 1))
    }

  }

  implicit object PrincipalMeridianEnum 
      extends IntRangeEnum[PrincipalMeridianT] {
    override val minVal = 1
    override val maxVal = 45
    override val tagged = PrincipalMeridian.apply
  }
}
