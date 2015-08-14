// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scalaz._
import Scalaz._

trait IntRangeEnum[T] extends Enum[Int @@ T] {

  val minVal: Int

  val maxVal: Int

  val tagged: PartialFunction[Int, Int @@ T]

  lazy val range = maxVal - minVal + 1

  override def order(x: Int @@ T, y: Int @@ T): Ordering =
    Tag.unwrap(x) ?|? Tag.unwrap(y)

  override def pred(a: Int @@ T): Int @@ T =
    tagged((Tag.unwrap(a) - minVal + range - 1) % range + minVal)

  override def succ(a: Int @@ T): Int @@ T =
    tagged((Tag.unwrap(a) - minVal + 1) % range + minVal)

  override def min: Option[Int @@ T] = tagged(minVal).some

  override def max: Option[Int @@ T] = tagged(maxVal).some
}
