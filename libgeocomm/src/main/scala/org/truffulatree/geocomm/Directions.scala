package org.truffulatree.geocomm

object Directions {

  private[this] val north = "N"
  private[this] val south = "S"
  private[this] val east = "E"
  private[this] val west = "W"
  private[this] val northeast = "NE"
  private[this] val southeast = "SE"
  private[this] val southwest = "SW"
  private[this] val northwest = "NW"

  trait D
  trait NS extends D
  trait EW extends D
  trait Corner extends D
  case object North extends NS { override def toString: String = north }
  case object South extends NS { override def toString: String = south }
  case object East extends EW { override def toString: String = east }
  case object West extends EW { override def toString: String = west }
  case object NorthWest extends Corner {
    override def toString: String = northwest
  }
  case object NorthEast extends Corner {
    override def toString: String = northeast
  }
  case object SouthEast extends Corner {
    override def toString: String = southeast
  }
  case object SouthWest extends Corner {
    override def toString: String = southwest
  }

  val ns: PartialFunction[String, NS] = (s: String) =>
  s.toUpperCase match {
    case `north` => North
    case `south` => South
  }

  val ew: PartialFunction[String, EW] = (s: String) =>
  s.toUpperCase match {
    case `east` => East
    case `west` => West
  }

  val corner: PartialFunction[String, Corner] = (s: String) =>
  s.toUpperCase match {
    case `northwest` => NorthWest
    case `northeast` => NorthEast
    case `southeast` => SouthEast
    case `southwest` => SouthWest
  }

  def apply(str: String): D =
    (ns orElse ew orElse corner)(str)

  val division: IndexedSeq[Corner] =
    IndexedSeq(NorthWest, NorthEast, SouthWest, SouthEast)
}
