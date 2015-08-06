// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
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
