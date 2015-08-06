package org.truffulatree.geocomm

import scala.language.higherKinds
import scala.concurrent.ExecutionContext
import scalaz._
import iteratee._
import iteratee.{ Iteratee => I }
import concurrent._
import effect._
import Scalaz._

object TaskCompletion {

  private[this] final case class Completions[A,G[_]:Traverse](
    pending: MVar[Int],
    optLatch: MVar[Option[BooleanLatch]],
    chan: Chan[Option[G[ThrowablesOr[A]]]]) {

    def addTask(gttha: G[Task[ThrowablesOr[A]]]): IO[Completions[A,G]] =
      IO(synchronized {
        pending.modify(p => IO((p + 1, ())))
      }).join.map { _ =>
        gttha.map(_.runAsync {
          case \/-(tha) =>
            unsafeAddValue((gttha map (_ => tha)).some)
          case -\/(f) =>
            unsafeAddValue((gttha map (_ => NonEmptyList(f).left[A])).some)
        })
        this
      }

    private[this] def unsafeAddValue(v: Option[G[ThrowablesOr[A]]]): Unit =
      IO(synchronized {
        chan.write(v) >>
        pending.modify(p => IO((p - 1, p == 1))) >>= { atZero =>
          optLatch.modify { ol =>
            IO {
              if (atZero)
                ol.foreach(_.release())
              (ol, ())
            }
          }
        }
      }).join.unsafePerformIO

    def addEndSentinel: IO[Chan[Option[G[ThrowablesOr[A]]]]] =
      IO(synchronized {
        pending.read >>= { np =>
          if (np > 0) {
            optLatch.modify { _ =>
              val latch = BooleanLatch()
              IO[(Option[BooleanLatch], BooleanLatch)]((latch.some, latch))
            } map { latch =>
              Task(latch.await())
            }
          } else {
            IO(Task.now(()))
          }
        }
      }).join.map { t =>
        t.runAsync { _ => unsafeAddValue(none) }
        chan
      }
  }

  def collect[A,G[_]:Traverse1](implicit ec: ExecutionContext):
      IterateeT[G[Task[ThrowablesOr[A]]], IO,
        Chan[Option[G[ThrowablesOr[A]]]]] = {

    def step(io: IO[Completions[A,G]]):
        (Input[G[Task[ThrowablesOr[A]]]]
          => IterateeT[G[Task[ThrowablesOr[A]]], IO,
            Chan[Option[G[ThrowablesOr[A]]]]]) =
      in => in(
        el = { gttha =>
          I.cont(step(io >>= (_.addTask(gttha))))
        },
        empty = I.cont(step(io)),
        eof = {
          val newIo = io >>= (_.addEndSentinel)
          IterateeT(newIo.map(r => I.sdone(r, I.emptyInput)))
        })

        I.cont {
          val initIo =
            for {
              p <- MVar.newMVar(0)
              ol <- MVar.newMVar(none[BooleanLatch])
              ch <- Chan.newChan[Option[G[ThrowablesOr[A]]]]
            } yield Completions(p, ol, ch)
          step(initIo)
        }
  }
}
