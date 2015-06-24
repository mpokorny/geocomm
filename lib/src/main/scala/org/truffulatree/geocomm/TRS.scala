package org.truffulatree.geocomm


case class TRS(
  state: States.State, 
  principalMeridian: PrincipalMeridians.PM,
  townshipNumber: Int,
  townshipFraction: TRS.Fractions.Fraction,
  townshipDirection: Directions.NS,
  rangeNumber: Int,
  rangeFraction: TRS.Fractions.Fraction,
  rangeDirection: Directions.EW,
  sectionNumber: TRS.Sections.Section,
  sectionDivision: List[Directions.Corner], // most to least significant
  townshipDuplicate: Int)

object TRS {
  object Fractions extends Enumeration {
    type Fraction = Value
    val Zero, One, Two, Three = Value
  }

  object Sections extends Enumeration(1) {
    type Section = Value
    val One, Two, Three, Four, Five, Six = Value
    val Seven, Eight, Nine, Ten, Eleven, Twelve = Value
    val Thirteen, Fourteen, Fifteen, Sixteen, Seventeen, Eighteen = Value
    val Nineteen, Twenty, TwentyOne, TwentyTwo, TwentyThree, TwentyFour = Value
    val TwentyFive, TwentySix, TwentySeven, TwentyEight, TwentyNine, Thirty = Value
    val ThirtyOne, ThirtyTwo, ThirtyThree, ThirtyFour, ThirtyFive, ThirtySix = Value
  }
}
