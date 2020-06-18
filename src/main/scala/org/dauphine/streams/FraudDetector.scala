package org.dauphine.streams

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

  /**
   * Monitors a stream of clicks events keyed by IP to output fraudulent events.
   * Events are fraudulent if the number of clicks aggregated by IP during a window is above a threshold.
   *
   * @param ipStream_clicks stream of events in the form (ip, 1, clickEvent)
   * @param MaxNbClicks threshold for the clicks aggregated by ip ratio
   * @param WindowTimeSecs window duration (in seconds) for aggregation
   * @return fraudulent events stream
   */
  def clickByIpFraud (ipStream_clicks : DataStream[(String, Int, String)], MaxNbClicks:Int, WindowTimeSecs:Long) : DataStream[String] = {
    // Count number of clicks by IP during window
    val clicks_count: DataStream[(String, Int, String)] = ipStream_clicks
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + "," + v2._3) }

    // Filter IPs with above-threshold clicks count and output their events
    val clicks_count_fraud: DataStream[String] = clicks_count
      .filter { value => value._2 >= MaxNbClicks }
      .map(value => value._3)
    return clicks_count_fraud
  }

  /**
   * Monitors a stream of display events keyed by IP to output fraudulent events.
   * Events are fraudulent if the number of displays aggregated by IP during a window is above a threshold.
   *
   * @param ipStream_displays stream of events in the form (ip, 1, displayEvent)
   * @param MaxNbDisplays threshold for the displays aggregated by IP ratio
   * @param WindowTimeSecs window duration (in seconds) for aggregation
   * @return fraudulent events stream
   */
  def displaysByIpFraud (ipStream_displays : DataStream[(String, Int, String)], MaxNbDisplays:Int, WindowTimeSecs:Long) : DataStream[String] = {
    // Count number of displays by IP during window
    val displays_count: DataStream[(String, Int, String)] = ipStream_displays
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + "," + v2._3) }

    // Filter IPs with above-threshold displays count and output their events
    val displays_count_fraud: DataStream[String] = displays_count
      .filter { value => value._2 >= MaxNbDisplays }
      .map(value => value._3)
    return displays_count_fraud
  }

  /**
   * Monitors a stream of clicks events and displays counts, both keyed by UID, to output fraudulent events.
   * Events are fraudulent if the CTR of the corresponding UID, aggregated during a window, meets some conditions.
   * Some tolerance is accepted to modulate CTR thresholds according to the number of displays the UID saw during the window.
   *
   *
   * @param uidStream_displays stream of displays count by UID in the form (UID, 1)
   * @param uidStream_clicks stream of clicks events in the form (UID, 1, clickEvent)
   * @param max_ctr_1 CTR threshold, applied when the tolerance_displays_1 criterion is met
   * @param max_ctr_2 CTR threshold, applied when the tolerance_displays_2 criterion is met
   * @param tolerance_displays_1 minimum number of displays by UID to apply the first CTR threshold
   * @param tolerance_displays_2 minimum number of displays by UID to apply the second CTR threshold
   * @param WindowTimeSecs window duration (in seconds) for aggregation
   * @return
   */
  def ctrByUidFraud(uidStream_displays: DataStream[(String, Int)],
                    uidStream_clicks: DataStream[(String, Int, String)],
                    max_ctr_1: Double, max_ctr_2: Double, tolerance_displays_1: Int, tolerance_displays_2: Int,
                    WindowTimeSecs: Long): DataStream[String] = {
    // Count number of displays by UID during window
    val displays_count_uid: DataStream[(String, Int)] = uidStream_displays
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }

    // Count number of clicks by UID during window (and append respective click events)
    val clicks_count_uid: DataStream[(String, Int, String)] = uidStream_clicks
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2, v1._3 + "," + v2._3) }

    // Join both counts to compute CTR by UID during window
    val streamJoined: DataStream[(String, Int, Int, Double, String)] = displays_count_uid.join(clicks_count_uid)
      .where(value => value._1).equalTo(value => value._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(WindowTimeSecs)))
      .apply { (e1, e2) => (e1._1, e2._2, e1._2, e2._2 / e1._2.toDouble, e2._3) }

    // Filter UID with above-threshold CTR and output their events
    val ctr_fraud = streamJoined
      .filter { value =>
        (value._2 > tolerance_displays_1 && ((value._2 <= tolerance_displays_2 && value._4 > max_ctr_1) || (value._2 > tolerance_displays_2 && value._4 > max_ctr_2)))
      }
      .map(value => value._5)

    return ctr_fraud
  }

  /**
   * This job monitors two streams of clicks and displays events from Kakfa.
   * Three patterns with tumbling windows are identified to detect fraudulent events.
   * Each pattern outputs its fraudulent events to a specific file.
   */
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

    // Set up event streams
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

    // Format input to prepare for count by IP or UID
    val ipStream_clicks : DataStream[(String, Int, String)] = streamEventTime_clicks.map(value => (value.get("ip").asText(), 1, value.toString))
    val ipStream_displays : DataStream[(String, Int, String)] = streamEventTime_displays.map(value => (value.get("ip").asText(), 1, value.toString))
    val uidStream_clicks : DataStream[(String, Int, String)] = streamEventTime_clicks.map(value => (value.get("uid").asText(), 1, value.toString))
    val uidStream_displays : DataStream[(String, Int)] = streamEventTime_displays.map(value => (value.get("uid").asText(), 1))

    // Set event time window length (1 hour)
    val WindowTimeSecs = 60 // 3600

    // PATTERN 1: monitor nb clicks by IP in window
    // Outputs clicks events whose IPs aggregated to more than 6 clicks during an hour
    val clicks_count_fraud = clickByIpFraud(ipStream_clicks, MaxNbClicks=6, WindowTimeSecs=WindowTimeSecs)
    clicks_count_fraud
      .rebalance.writeAsText("output/clicks_fraud_events.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    // PATTERN 2: monitor nb displays by IP in window
    // Outputs displays events whose IPs aggregated to more than 15 displays during an hour
    val displays_count_fraud = displaysByIpFraud(ipStream_displays, MaxNbDisplays=15, WindowTimeSecs)
    displays_count_fraud
      .rebalance.writeAsText("output/displays_fraud_events.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    // PATTERN 3: monitor CTR by UID in window
    // Output clicks events whose UID was shown more than 2 displays and :
    // - either had an aggregated CTR over 0.51 during the window (with less than 10 displays in total)
    // - either had an aggregated CTR over 0.26 during the window (with more than 10 displays in total)
    val ctr_fraud = ctrByUidFraud (uidStream_displays, uidStream_clicks, max_ctr_1=0.51, max_ctr_2=0.26, tolerance_displays_1=2, tolerance_displays_2=10, WindowTimeSecs=WindowTimeSecs)
    ctr_fraud
      .rebalance.writeAsText("output/ctr_fraud_events.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    // Execute program
    env.execute("Fraud detection")
  }
}
