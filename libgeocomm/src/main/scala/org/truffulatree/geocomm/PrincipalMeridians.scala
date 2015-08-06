// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

object PrincipalMeridians extends Enumeration(1) {
  type PM = Value
  val First, Second, Third, Fourth, Fifth, Sixth = Value
  val BlackHills, Boise, Chickasaw, Choctaw, Cimarron = Value
  val CopperRiver, Fairbanks, GilaAndSaltRiver, Humboldt = Value
  val Hunstville, Indian, Louisiana, Michigan, Montana = Value
  val MountDiablo, Navajo, NewMexico, StHelens, StStephens = Value
  val SaltLake, SanBernardino, Seward, Tallahassee, UintahSpecial = Value
  val Ute, Washington, Willamette, WindRiver, Ohio, Invalid = Value
  val MuskigumRiver, OhioRiver, FirstSciotoRiver, SecondSciotoRiver = Value
  val ThirdSciotoRiver, EllicottsLine, TwelveMileSquare = Value
  val KateelRiver, Umiat = Value
}
