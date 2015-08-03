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
    case "n" => North
    case "S" => South
    case "s" => South
  }

  val ew: PartialFunction[String, EW] = {
    case "E" => East
    case "e" => East
    case "W" => West
    case "w" => West
  }

  val corner: PartialFunction[String, Corner] = {
    case "NW" => NorthWest
    case "nw" => NorthWest
    case "NE" => NorthEast
    case "ne" => NorthEast
    case "SE" => SouthEast
    case "se" => SouthEast
    case "SW" => SouthWest
    case "sw" => SouthWest
  }

  def apply(str: String): D =
    (ns orElse ew orElse corner)(str)

  val division: IndexedSeq[Corner] =
    IndexedSeq(NorthWest, NorthEast, SouthWest, SouthEast)
}
