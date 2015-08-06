package org.truffulatree.geocomm

import scalaz._
import Scalaz._

case class TRS(
  state: States.State,
  principalMeridian: PrincipalMeridians.PM,
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
    case (i) if FractionEnum.minVal <= i && i <= FractionEnum.maxVal =>
      Tag[Int, FractionT](i)
  }

  implicit object FractionEnum extends Enum[Fraction] {

    val minVal = 0

    val maxVal = 3

    private[this] val range = maxVal - minVal + 1

    override def order(x: Fraction, y: Fraction): Ordering =
      Tag.unwrap(x) ?|? Tag.unwrap(y)

    override def pred(a: Fraction): Fraction =
      Fraction((Tag.unwrap(a) - minVal + range - 1) % range + minVal)

    override def succ(a: Fraction): Fraction =
      Fraction((Tag.unwrap(a) - minVal + 1) % range + minVal)

    override def min: Option[Fraction] = Fraction(minVal).some

    override def max: Option[Fraction] = Fraction(maxVal).some
  }

  sealed trait SectionT

  type Section = Int @@ SectionT

  def Section: PartialFunction[Int, Section] = {
    case (i) if SectionEnum.minVal <= i && i <= SectionEnum.maxVal =>
      Tag[Int, SectionT](i)
  }

  implicit object SectionEnum extends Enum[Section] {

    val minVal = 1

    val maxVal = 36

    private[this] val range = maxVal - minVal + 1

    override def order(x: Section, y: Section): Ordering =
      Tag.unwrap(x) ?|? Tag.unwrap(y)

    override def pred(a: Section): Section =
      Section((Tag.unwrap(a) - minVal + range - 1) % range + minVal)

    override def succ(a: Section): Section =
      Section((Tag.unwrap(a) - minVal + 1) % range + minVal)

    override def min: Option[Section] = Section(minVal).some

    override def max: Option[Section] = Section(maxVal).some
  }
}
