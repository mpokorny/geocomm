// Copyright 2015 Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.geocomm

import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scalaz._
import iteratee._
import concurrent._
import effect._
import Scalaz._

class TaskCompletionSpec extends UnitSpec {

  "A TaskCompletion iteratee" should {
    "provide values of successfully completed tasks" in {
      val ints = (0 until 10).toList
      val tasks = ints map (i => Task.now[ThrowablesOr[Int]](i.right))
      val taskEnum = Iteratee.enumList[Task[ThrowablesOr[Int]],IO](tasks)
      val taskCompletion = TaskCompletion.collect[Int,Id]
      def accResults(ch: Chan[Option[ThrowablesOr[Int]]], acc: List[Int]):
          IO[(Chan[Option[ThrowablesOr[Int]]], List[Int])] =
        ch.read >>= {
          case Some(\/-(r)) => accResults(ch, r :: acc)
          case _ => IO((ch, acc.reverse))
        }
      ((taskCompletion &= taskEnum).run >>= (accResults(_, List.empty))).
          unsafePerformIO match {
            case (_, res) =>
              res should contain theSameElementsInOrderAs ints
          }
    }

    "provide results of unsuccessfully completed tasks" in {
      val ints = (0 until 10).toList
      val thInts: List[ThrowablesOr[Int]] = ints map { i =>
        if (i % 2 == 0) i.right
        else NonEmptyList(new Exception(i.shows)).left
      }
      val tasks = thInts map (Task.now _)
      val taskEnum = Iteratee.enumList[Task[ThrowablesOr[Int]],IO](tasks)
      val taskCompletion = TaskCompletion.collect[Int,Id]
      def accResults(
        ch: Chan[Option[ThrowablesOr[Int]]],
        acc: List[ThrowablesOr[Int]]):
          IO[(Chan[Option[ThrowablesOr[Int]]], List[ThrowablesOr[Int]])] =
        ch.read >>= {
          case Some(r) => accResults(ch, r :: acc)
          case _ => IO((ch, acc.reverse))
        }
      ((taskCompletion &= taskEnum).run >>= (accResults(_, List.empty))).
          unsafePerformIO match {
            case (_, acc) =>
              acc should contain theSameElementsInOrderAs thInts
          }
    }

    "provide results as they become available" in {
      val ints = (0 until 4).toList
      val tasks = ints map { i =>
        Task.fork {
          Task.async[ThrowablesOr[Int]] { cb =>
            // delay decreases with int value, so results should appear in Chan
            // in reverse order
            Thread.sleep((3 - i) * 500)
            cb(i.right.right)
          }
        }
      }
      val taskEnum = Iteratee.enumList[Task[ThrowablesOr[Int]],IO](tasks)
      val taskCompletion = TaskCompletion.collect[Int,Id]
      def accResults(ch: Chan[Option[ThrowablesOr[Int]]], acc: List[Int]):
          IO[(Chan[Option[ThrowablesOr[Int]]], List[Int])] =
        ch.read >>= {
          case Some(\/-(r)) => accResults(ch, r :: acc)
          case _ => IO((ch, acc.reverse))
        }
      ((taskCompletion &= taskEnum).run >>= (accResults(_, List.empty))).
          unsafePerformIO match {
            case (_, res) =>
              res should contain theSameElementsInOrderAs ints.reverse
          }
    }

    "flatten task failures into result failure" in {
      val ints = (0 until 10).toList
      def ex(i: Int): Exception = new Exception(i.shows)
      val thInts: List[ThrowablesOr[Int]] = ints map { i =>
        if (i % 2 == 0) i.right
        else NonEmptyList(ex(i)).left
      }
      val tasks: List[Task[ThrowablesOr[Int]]] = thInts map {
        case \/-(i) => Task.now(i.right)
        case -\/(ths) => Task.fail(ths.head)
      }
      val taskEnum = Iteratee.enumList[Task[ThrowablesOr[Int]],IO](tasks)
      val taskCompletion = TaskCompletion.collect[Int,Id]
      def accResults(
        ch: Chan[Option[ThrowablesOr[Int]]],
        acc: List[ThrowablesOr[Int]]):
          IO[(Chan[Option[ThrowablesOr[Int]]], List[ThrowablesOr[Int]])] =
        ch.read >>= {
          case Some(r) => accResults(ch, r :: acc)
          case _ => IO((ch, acc.reverse))
        }
      ((taskCompletion &= taskEnum).run >>= (accResults(_, List.empty))).
          unsafePerformIO match {
            case (_, acc) =>
              acc should contain theSameElementsInOrderAs thInts
          }
    }
  }
}
