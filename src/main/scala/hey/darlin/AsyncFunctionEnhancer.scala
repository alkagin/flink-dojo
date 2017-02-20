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
package hey.darlin

import java.util.concurrent.TimeUnit

import hey.darlin.datastream._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.AsyncCollector

import scala.concurrent.{ExecutionContext, Future}

object AsyncFunctionEnhancer {

  implicit lazy val executor = ExecutionContext.fromExecutor(Executors.directExecutor())

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)

    val hostName = params.get("host", "localhost")
    val port = params.getInt("port", 9999)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream(hostName, port)

    val dataStream = source.unorderedWait[String](1L, TimeUnit.SECONDS) { (in: String, c: AsyncCollector[String]) =>
      Future.successful("procesedAsync_" + in).onSuccess {
        case s => c.collect(Iterable(s))
      }
    }

    dataStream.print()

    env.execute("AsyncFunctionEnhancer")
  }
}
