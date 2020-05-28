/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart
import java.util.Properties

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object FraudDetector {
  def main(args: Array[String]) {

    // Set up the streaming execution environment

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181") // only required for Kafka 0.8
    properties.setProperty("group.id", "test")

    val kafkaConsumer = new FlinkKafkaConsumer[ObjectNode](
      "clicks",
      new JSONKeyValueDeserializationSchema(false),
      properties
    )

    // Set up the event stream

    // val stream = env.addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))
    val stream : DataStream[ObjectNode] = env.addSource(kafkaConsumer).name("clicks")
    val streamValue : DataStream[JsonNode] = stream.map(value => value.get("value"))
    val streamWithTimestampsAndWatermarks = streamValue.assignAscendingTimestamps( _.get("timestamp").asLong())

    streamWithTimestampsAndWatermarks.print()

    // Aggregate on stream keyed by IP

    /**
     * The accumulator is used to keep a running sum and a count. The [getResult] method
     * computes the average.
     */
    class AverageAggregate extends AggregateFunction[JsonNode, Long, Double] {
      override def createAccumulator() = 0

      override def add(value: JsonNode, accumulator: Long) = accumulator + 1

      override def getResult(accumulator: Long) = accumulator

      override def merge(a: Long, b: Long) =  a + b
    }

    val count = streamWithTimestampsAndWatermarks
      .keyBy(_.get("ip"))
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(new AverageAggregate)

    // Execute program
    env.execute("Fraud detection")
  }
}
