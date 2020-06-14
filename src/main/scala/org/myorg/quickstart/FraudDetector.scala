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
    val ipStream_clicks : DataStream[(String, Int, String)] = streamEventTime_clicks.map(value => (value.get("ip").asText(), 1, value.toString))
    val ipStream_displays : DataStream[(String, Int, String)] = streamEventTime_displays.map(value => (value.get("ip").asText(), 1, value.toString))
    val uidStream_clicks : DataStream[(String, Int, String)] = streamEventTime_clicks.map(value => (value.get("uid").asText(), 1, value.toString))
    val uidStream_displays : DataStream[(String, Int)] = streamEventTime_displays.map(value => (value.get("uid").asText(), 1))

    // Set event time window length
    val WindowTimeSecs = 3600

    // PATTERN 1: nb clicks / IP
    // TODO: Should output to proper Flink Sink (with checkpointing)
    val MaxNbClicks = 6
    val clicks_count : DataStream[(String, Int, String)] = ipStream_clicks
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + "," + v2._3) }
    val clicks_count_fraud : DataStream[String] = clicks_count
      .filter {value => value._2 >= MaxNbClicks}
      .map(value => value._3)
    clicks_count_fraud
      .rebalance.writeAsText("output/clicks_fraud_events.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    // PATTERN 2: nb displays / IP
    // TODO: Should output to proper Flink Sink (with checkpointing)
    val MaxNbDisplays = 15
    val displays_count : DataStream[(String, Int, String)] = ipStream_displays
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + "," + v2._3) }
    val displays_count_fraud : DataStream[String] = displays_count
      .filter {value => value._2 >= MaxNbDisplays}
      .map(value => value._3)
    displays_count_fraud
      .rebalance.writeAsText("output/displays_fraud_events.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    // PATTERN 3: CTR / UID
    // TODO: Should output to proper Flink Sink (with checkpointing)
    val displays_count_uid : DataStream[(String, Int)] = uidStream_displays
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
    val clicks_count_uid : DataStream[(String, Int, String)] = uidStream_clicks
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + "," + v2._3) }

    val max_ctr_1 = 0.51
    val max_ctr_2 = 0.26
    val tolerance_displays_1 = 2
    val tolerance_displays_2 = 10

    val streamJoined : DataStream[(String, Int, Int, Double, String)] = displays_count_uid.join(clicks_count_uid)
      .where(value => value._1).equalTo(value => value._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .apply { (e1, e2) => (e1._1, e2._2, e1._2, e2._2 / e1._2.toDouble, e2._3)}

    val ctr_fraud = streamJoined
      .filter {value => (value._2 > tolerance_displays_1 &&
        ((value._2 <= tolerance_displays_2 && value._4 > max_ctr_1) || (value._2 > tolerance_displays_2 && value._4 > max_ctr_2))
        )}
        .map(value => value._5)
    ctr_fraud
      .rebalance.writeAsText("output/ctr_fraud_events.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    // Tmp debug file
    val ctr_debug = streamJoined
      .filter {value => (value._2 > tolerance_displays_1 &&
        ((value._2 <= tolerance_displays_2 && value._4 > max_ctr_1) || (value._2 > tolerance_displays_2 && value._4 > max_ctr_2))
        )}
      
      .map(value => (value._1, value._2, value._3, value._4))
    ctr_debug
      .rebalance.writeAsText("output/ctr_fraud_events_debug_uid.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    // Execute program
    env.execute("Fraud detection")
  }
}
