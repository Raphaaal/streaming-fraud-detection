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

import org.apache.flink.configuration.{ConfigConstants, Configuration, RestOptions}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

import scala.math.max


object FraudDetector {
  def main(args: Array[String]) {

    // Set up the streaming execution environment
    val config = new Configuration()
    config.setInteger(RestOptions.PORT, 8082)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
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

    val stream : DataStream[ObjectNode] = env.addSource(kafkaConsumer).name("clicks")
    val streamValue : DataStream[JsonNode] = stream.map {value => value.get("value") }

    // Set up event time with watermarks

    class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[JsonNode] {

      override def extractTimestamp(element: JsonNode, previousElementTimestamp: Long): Long = {
        element.get("timestamp").asLong()
      }

      override def checkAndGetNextWatermark(lastElement: JsonNode, extractedTimestamp: Long): Watermark = {
        new Watermark(extractedTimestamp)
      }
    }

    val streamEventTime : DataStream[JsonNode] = streamValue.assignTimestampsAndWatermarks(new PunctuatedAssigner)

    // Aggregate on stream keyed by IP

    val ipStream : DataStream[(String, Int)] = streamEventTime.map(value => (value.get("ip").asText(), 1))
    val clicks_count : DataStream[(String, Int)] = ipStream
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) // .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
      //.filter {value => value._2 >= 6}

    clicks_count.print

    // Execute program
    env.execute("Fraud detection")
  }
}
