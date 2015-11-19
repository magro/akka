/**
 * Copyright (C) 2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import akka.stream._
import akka.stream.impl.Stages.StageModule
import language.higherKinds
import scala.annotation.unchecked.uncheckedVariance

/**
 * A “stream of streams” sub-flow of data elements, e.g. produced by `groupBy`.
 * SubFlows cannot contribute to the super-flow’s materialized value since they
 * are materialized later, during the runtime of the flow graph processing.
 */
trait SubFlow[+Out, +Mat, +F[+_], C] extends FlowOps[Out, Mat] {

  override type Repr[+T] = SubFlow[T, Mat @uncheckedVariance, F @uncheckedVariance, C @uncheckedVariance]
  override type Closed = C

  /**
   * Attach a [[Sink]] to each sub-flow, closing the overall Graph that is being
   * constructed.
   */
  def to[M](sink: Graph[SinkShape[Out], M]): C

  /**
   * Flatten the sub-flows back into the super-flow, performing a merge of the
   * given breadth. For concatenation of the flows use a breadth of 1 but beware
   * that this will lead to deadlocks if the sub-flows need to make progress
   * concurrently (as is generally the case when using `groupBy` on an unordered
   * input stream).
   */
  def mergeBack(breadth: Int): F[Out]
}
