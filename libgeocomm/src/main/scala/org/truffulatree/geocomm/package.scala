package org.truffulatree

import scalaz.{ NonEmptyList, \/ }

package object geocomm {
  type ThrowablesOr[A] = \/[NonEmptyList[Throwable], A]
}
