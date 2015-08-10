// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scalaz._
import iteratee._
import concurrent._
import effect._
import Scalaz._

class ChanEnumeratorSpec extends UnitSpec {

  "A ChanEnumerator" should {
    "provide all non-empty Option values in the input Chan" in {
      val ints = (0 until 10).toList
      val io =
        for {
          ch <- Chan.newChan[Option[Int]]
          _ <- ((ints map (i => ch.write(i.some))) :+ ch.write(none)).sequenceU
          is <- (Iteratee.collectT[Int, IO, List] &= ChanEnumerator(ch)).run
        } yield is
      io.unsafePerformIO should contain theSameElementsInOrderAs ints
    }

    "provide no values in an empty input Chan" in {
      val io =
        for {
          ch <- Chan.newChan[Option[Int]]
          _ <- ch.write(none)
          is <- (Iteratee.collectT[Int, IO, List] &= ChanEnumerator(ch)).run
        } yield is
      io.unsafePerformIO shouldBe empty
    }
  }
}
