package org.truffulatree.geocomm

import scala.language.higherKinds
import scala.concurrent.{ ExecutionContext, Future, blocking }
import scala.util.{ Failure, Success }
import scalaz._
import iteratee._
import iteratee.{ Iteratee => I }
import concurrent.{ MVar, Chan, BooleanLatch }
import effect._
import Scalaz._

object FutureCompletion {

  private final case class Completions[A,G[_]](
    pending: MVar[Int],
    optLatch: MVar[Option[BooleanLatch]],
    chan: Chan[Option[G[ThrowablesOr[A]]]]) {

    def addFuture: IO[Unit] =
      IO(synchronized {
        pending.modify(p => IO((p + 1, ())))
      }).join

    def addValue(v: Option[G[ThrowablesOr[A]]]): IO[Unit] = 
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
      }).join

    def await(implicit ec: ExecutionContext): IO[Future[Unit]] =
      IO(synchronized {
        pending.read >>= { np =>
          if (np > 0) {
            optLatch.modify { _ =>
              val latch = BooleanLatch()
              IO[(Option[BooleanLatch], BooleanLatch)]((latch.some, latch))
            } map { latch =>
              Future(blocking { latch.await() })
            }
          } else {
            IO(Future.successful(()))
          }
        }
      }).join
  }

  def collect[A,G[_]:Traverse](onEmpty: G[Future[ThrowablesOr[A]]] => IO[Unit])(
    implicit ec: ExecutionContext):
      IterateeT[G[Future[ThrowablesOr[A]]], IO, Chan[Option[G[ThrowablesOr[A]]]]] = {

    def step(io: IO[Completions[A,G]]):
        (Input[G[Future[ThrowablesOr[A]]]]
          => IterateeT[G[Future[ThrowablesOr[A]]], IO, Chan[Option[G[ThrowablesOr[A]]]]]) =
      in => in(
        el = { gftha =>
          val newIo =
            if (!gftha.empty) {
              gftha.foldLeft(io) { (io1, ftha) =>
                io1 >>= { completions =>
                  for {
                    _ <- completions.addFuture
                    _ <- IO {
                      ftha onComplete {
                        case Success(s) =>
                          completions.addValue(gftha.map(_ => s).some).
                            unsafePerformIO
                        case Failure(f) =>
                          completions.addValue(
                            gftha.map(_ => NonEmptyList(f).left[A]).some).
                            unsafePerformIO
                      }
                    }
                  } yield completions
                }
              }
            } else {
              io >>= (io1 => onEmpty(gftha).map(_ => io1))
            }

          I.cont(step(newIo))
        },
        empty = I.cont(step(io)),
        eof = {
          val newIo = io >>= { completions =>
            for {
              f <- completions.await
              _ <- IO {
                f onComplete { _ =>
                  completions.addValue(none).unsafePerformIO
                }
              }
            } yield completions.chan
          }
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
