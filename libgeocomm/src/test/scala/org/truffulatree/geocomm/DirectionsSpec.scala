// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

class DirectionsSpec extends UnitSpec {
  val n = "N"
  val e = "E"
  val s = "S"
  val w = "W"
  val nw = "NW"
  val ne = "NE"
  val se = "SE"
  val sw = "SW"

  "Directions" should {
    "have conventional string abbreviation representations" in {
      Directions.North.toString should equal (n)
      Directions.East.toString should equal (e)
      Directions.South.toString should equal (s)
      Directions.West.toString should equal (w)
      Directions.NorthWest.toString should equal (nw)
      Directions.NorthEast.toString should equal (ne)
      Directions.SouthEast.toString should equal (se)
      Directions.SouthWest.toString should equal (sw)
    }

    "should convert from string abbreviations to Enumeration value" in {
      Directions(n) should equal (Directions.North)
      Directions(n.toLowerCase) should equal (Directions.North)
      Directions(e) should equal (Directions.East)
      Directions(e.toLowerCase) should equal (Directions.East)
      Directions(s) should equal (Directions.South)
      Directions(s.toLowerCase) should equal (Directions.South)
      Directions(w) should equal (Directions.West)
      Directions(w.toLowerCase) should equal (Directions.West)
      Directions(nw) should equal (Directions.NorthWest)
      Directions(nw.toLowerCase) should equal (Directions.NorthWest)
      Directions(sw) should equal (Directions.SouthWest)
      Directions(sw.toLowerCase) should equal (Directions.SouthWest)
      Directions(ne) should equal (Directions.NorthEast)
      Directions(ne.toLowerCase) should equal (Directions.NorthEast)
      Directions(se) should equal (Directions.SouthEast)
      Directions(se.toLowerCase) should equal (Directions.SouthEast)
    }
  }
}
