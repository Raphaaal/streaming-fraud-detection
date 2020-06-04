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
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}



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
    val streamValue : DataStream[JsonNode] = stream.map {value => value.get("value") }
    //val streamWithTimestampsAndWatermarks : DataStream[JsonNode] = streamValue.assignAscendingTimestamps( _.get("timestamp").asLong())

    // Aggregate on stream keyed by IP
    val ipStream : DataStream[(String, Int)] = streamValue.map(value => (value.get("ip").asText(), 1))
     val clicks_count : DataStream[(String, Int)] = ipStream
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
      .filter {value => value._2 >= 6}
    //.window(TumblingEventTimeWindows.of(Time.seconds(10)))

    clicks_count.print

    // Execute program
    env.execute("Fraud detection")
  }
}
