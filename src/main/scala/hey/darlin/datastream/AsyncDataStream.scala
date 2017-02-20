/*
 * Copyright (c) 2017 Andrea Sella
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hey.darlin.datastream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{AsyncDataStream => JavaADS}
import org.apache.flink.streaming.api.functions.async.collector.{AsyncCollector => JavaAsyncCollector}
import org.apache.flink.streaming.api.functions.async.{AsyncFunction => JavaAsyncFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{AsyncCollector, AsyncFunction, JavaAsyncCollectorWrapper}

import scala.concurrent.duration.TimeUnit

class AsyncDataStream[I: TypeInformation](input: DataStream[I]) {

  private val DefaultQueueCapacity = 100

  final def unorderedWait[O](timeout: Long, timeUnit: TimeUnit, capacity: Int = DefaultQueueCapacity)(
    asyncFunction: AsyncFunctionMagnet[I, O])(implicit outType: TypeInformation[O]): DataStream[O] =
    new DataStream[O](
      JavaADS.unorderedWait[I, O](input.javaStream, asyncFunction(), timeout, timeUnit, capacity).returns(outType)
    )

  final def orderedWait[O](timeout: Long, timeUnit: TimeUnit, capacity: Int = DefaultQueueCapacity)(
    asyncFunction: AsyncFunctionMagnet[I, O])(implicit outType: TypeInformation[O]): DataStream[O] =
    new DataStream[O](
      JavaADS.orderedWait[I, O](input.javaStream, asyncFunction(), timeout, timeUnit, capacity).returns(outType)
    )
}

sealed trait AsyncFunctionMagnet[IN, OUT] {
  def apply(): JavaAsyncFunction[IN, OUT]
}

object AsyncFunctionMagnet {

  type Function[IN, OUT] = (IN, AsyncCollector[OUT]) => Unit

  private def scalaClean[F <: AnyRef](f: F): F = {
    //ClosureCleaner.clean(f, checkSerializable = true) //FIXME enable ClosureCleaner
    f
  }

  /** Magnet Pattern `AsyncFunction`
    */
  implicit def fromAsyncFunction[IN, OUT](asyncFunction: AsyncFunction[IN, OUT]) = new AsyncFunctionMagnet[IN, OUT] {
    override def apply() = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, collector: JavaAsyncCollector[OUT]): Unit = {
        asyncFunction.asyncInvoke(input, new JavaAsyncCollectorWrapper[OUT](collector))
      }
    }
  }

  /** Magnet Pattern `(IN, AsyncCollector[OUT]) => Unit)`
    */
  implicit def fromFunction[IN, OUT](asyncFunction: Function[IN, OUT]) = new AsyncFunctionMagnet[IN, OUT] {
    override def apply() = new JavaAsyncFunction[IN, OUT] {
      override def asyncInvoke(input: IN, collector: JavaAsyncCollector[OUT]): Unit = {
        scalaClean(asyncFunction)(input, new JavaAsyncCollectorWrapper[OUT](collector))
      }
    }
  }
}

