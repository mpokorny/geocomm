package org.truffulatree

import scalaz.{ NonEmptyList, \/ }

package object geocomm {
  type ThrowablesOr[A] = \/[NonEmptyList[Throwable], A]

  def splitAll(str: String, sep: Char): Array[String] =
    (str.foldLeft(Vector("")) {
      case (Vector(s), c) if c != sep => Vector(s + c)
      case (vector :+ s, c) if c != sep => vector :+ (s + c)
      case (vector, _) => vector :+ ""
    }).toArray
}
