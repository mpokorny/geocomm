package org.truffulatree.geocomm

class DirectionsSpec extends UnitSpec {
  "Directions" should {
    "have conventional string abbreviation representations" in {
      Directions.North.toString should equal ("N")
      Directions.East.toString should equal ("E")
      Directions.South.toString should equal ("S")
      Directions.West.toString should equal ("W")
      Directions.NorthWest.toString should equal ("NW")
      Directions.NorthEast.toString should equal ("NE")
      Directions.SouthEast.toString should equal ("SE")
      Directions.SouthWest.toString should equal ("SW")
    }

    "should convert from string abbreviations to Enumeration value" in {
      Directions("N") should equal (Directions.North)
      Directions("n") should equal (Directions.North)
      Directions("E") should equal (Directions.East)
      Directions("e") should equal (Directions.East)
      Directions("S") should equal (Directions.South)
      Directions("s") should equal (Directions.South)
      Directions("W") should equal (Directions.West)
      Directions("w") should equal (Directions.West)
      Directions("NW") should equal (Directions.NorthWest)
      Directions("nw") should equal (Directions.NorthWest)
      Directions("SW") should equal (Directions.SouthWest)
      Directions("sw") should equal (Directions.SouthWest)
      Directions("NE") should equal (Directions.NorthEast)
      Directions("ne") should equal (Directions.NorthEast)
      Directions("SE") should equal (Directions.SouthEast)
      Directions("se") should equal (Directions.SouthEast)
    }
  }
}
