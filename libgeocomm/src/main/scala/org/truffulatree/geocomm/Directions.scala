package org.truffulatree.geocomm

object Directions {
  trait D
  trait NS extends D
  trait EW extends D
  trait Corner extends D
  case object North extends NS { override def toString: String = "N" }
  case object South extends NS { override def toString: String = "S" }
  case object East extends EW { override def toString: String = "E" }
  case object West extends EW { override def toString: String = "W" }
  case object NorthWest extends Corner { override def toString: String = "NW" }
  case object NorthEast extends Corner { override def toString: String = "NE" }
  case object SouthEast extends Corner { override def toString: String = "SE" }
  case object SouthWest extends Corner { override def toString: String = "SW" }

  val ns: PartialFunction[String, NS] = {
    case "N" => North
    case "S" => South
  }

  val ew: PartialFunction[String, EW] = {
    case "E" => East
    case "W" => West
  }

  val corner: PartialFunction[String, Corner] = {
    case "NW" => NorthWest
    case "NE" => NorthEast
    case "SE" => SouthEast
    case "SW" => SouthWest
  }

  def apply(str: String): D =
    (ns orElse ew orElse corner)(str)

  val division: IndexedSeq[Corner] =
    IndexedSeq(NorthWest, NorthEast, SouthWest, SouthEast)
}
