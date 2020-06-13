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

import org.apache.flink.api.common.functions.CoGroupFunction
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
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

import scala.math.max


object FraudDetector {
  def main(args: Array[String]) {

    // Set up the streaming execution environment
    val config = new Configuration()
    config.setInteger(RestOptions.PORT, 8082)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config) // val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Set up sources properties
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    // Set up Kafka consumers sources
    val kafkaConsumer_clicks = new FlinkKafkaConsumer[ObjectNode](
      "clicks",
      new JSONKeyValueDeserializationSchema(false),
      properties
    )
    val kafkaConsumer_displays = new FlinkKafkaConsumer[ObjectNode](
      "displays",
      new JSONKeyValueDeserializationSchema(false),
      properties
    )

    // Set up the event streams
    val stream_clicks : DataStream[ObjectNode] = env.addSource(kafkaConsumer_clicks).name("clicks")
    val streamValue_clicks : DataStream[JsonNode] = stream_clicks.map {value => value.get("value") }
    val stream_displays : DataStream[ObjectNode] = env.addSource(kafkaConsumer_displays).name("displays")
    val streamValue_displays : DataStream[JsonNode] = stream_displays.map {value => value.get("value") }

    // Set up timestamps and watermarks
    class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[JsonNode] {
      override def extractTimestamp(element: JsonNode, previousElementTimestamp: Long): Long = {
        element.get("timestamp").asLong()*1000
      }
      override def checkAndGetNextWatermark(lastElement: JsonNode, extractedTimestamp: Long): Watermark = {
        new Watermark(extractedTimestamp)
      }
    }
    val streamEventTime_clicks : DataStream[JsonNode] = streamValue_clicks.assignTimestampsAndWatermarks(new PunctuatedAssigner)
    val streamEventTime_displays : DataStream[JsonNode] = streamValue_displays.assignTimestampsAndWatermarks(new PunctuatedAssigner)

    // Format input events
    val ipStream_clicks : DataStream[(String, Int)] = streamEventTime_clicks.map(value => (value.get("ip").asText(), 1))
    val ipStream_displays : DataStream[(String, Int)] = streamEventTime_displays.map(value => (value.get("ip").asText(), 1))
    val WindowTimeSecs = 3600

    // Aggregate on streams keyed by IP
    val clicks_count : DataStream[(String, Int)] = ipStream_clicks
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs))) // .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
    val displays_count : DataStream[(String, Int)] = ipStream_displays
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs))) // .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }

    // PATTERN 1: nb clicks / IP
    // TODO: Should output the full fraudulent event (not just the IP)
    val clicks_count_fraud : DataStream[(String, Int)] = clicks_count.filter {value => value._2 >= 6}
    clicks_count_fraud
      .rebalance.writeAsText("output/clicks_ip_output.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1) // Should output to proper Flink Sink (with checkpointing)

    // PATTERN 2: CTR / IP
    //  TODO: should be by UID and with specific thresolds
    val streamJoined : DataStream[(String, Double)] = displays_count.join(clicks_count)
      .where(value => value._1).equalTo(value => value._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .apply { (e1, e2) => (e1._1, 1.0 * (e2._2 / e1._2)) }
    val ctr_fraud = streamJoined.filter {value => value._2 >= 0.25}
    clicks_count_fraud
      .rebalance.writeAsText("output/ctr_ip_output.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1) // Should output to proper Flink Sink (with checkpointing)

    // PATTERN 3: nb displays / IP
    // TODO: Should output the full fraudulent event (not just the IP)
    val displays_count_fraud : DataStream[(String, Int)] = displays_count.filter {value => value._2 >= 6}
    displays_count_fraud
      .rebalance.writeAsText("output/displays_ip_output.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1) // Should output to proper Flink Sink (with checkpointing)
  /*
    // Aggregate on two joined streams
    class CTR extends CoGroupFunction[(String, Int), (String, Int), (String, Int, Int)] {

      override def coGroup(
                            leftElements : java.lang.Iterable[(String, Int)],
                            rightElements: java.lang.Iterable[(String, Int)],
                            out: Collector[(String, Int, Int)]
                          ): (String, Int, Int) = {
        val NullElement = -1

        for (leftElem <- leftElements) {
          var hadElements = false
          for (rightElem <- rightElements) {
            out.collect( (leftElem._0 , rightElem._1, rightElem._1) : (String, Int, Int) )
            hadElements = true
          }
          if (!hadElements) {
            out.collect( (leftElem._0 , None))
          }
        }
      }
    }

   */

    // Execute program
    env.execute("Fraud detection")
  }
}
