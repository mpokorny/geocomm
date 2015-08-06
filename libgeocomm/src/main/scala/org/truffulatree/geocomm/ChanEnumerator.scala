// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scalaz._
import iteratee._
import iteratee.{ Iteratee => I }
import effect._
import concurrent._
import Scalaz._

object ChanEnumerator {
  def apply[A](chan: Chan[Option[A]]): EnumeratorT[A, IO] = {

    def readOne[E]: IterateeT[E, IO, Option[A]] =
      IterateeT[E, IO, Option[A]](
        chan.read.map(oa => I.sdone(oa, I.emptyInput)))

    new EnumeratorT[A, IO] {
      override def apply[B]: StepT[A, IO, B] => IterateeT[A, IO, B] =
        (s: StepT[A, IO, B]) => {
          s.mapCont { k =>
            readOne.flatMap { oa =>
              oa map { a =>
                k(I.elInput(a)) >>== apply[B]
              } getOrElse {
                s.pointI
              }
            }
          }
        }
    }
  }
}
