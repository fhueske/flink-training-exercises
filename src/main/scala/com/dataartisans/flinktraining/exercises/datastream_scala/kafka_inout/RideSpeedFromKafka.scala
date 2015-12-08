/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_scala.kafka_inout

import java.util.Properties
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.TimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082
import org.apache.flink.util.Collector


/**
 * Scala reference implementation for the "Ride Speed" exercise of the Flink training (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to read taxi ride records from an Apache Kafka topic and compute the average speed of completed taxi rides.
 *
 */
object RideSpeedFromKafka {

  private val LOCAL_ZOOKEEPER_HOST = "localhost:2181"
  private val LOCAL_KAFKA_BROKER = "localhost:9092"
  private val RIDE_SPEED_GROUP = "rideSpeedGroup"

  @throws(classOf[Exception])
  def main(args: Array[String]) {

    val maxDelay = 60 // events are out of order by max 60 seconds

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // configure Kafka consumer
    val kafkaProps = new Properties
    kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER)
    kafkaProps.setProperty("group.id", RIDE_SPEED_GROUP)

    // create a TaxiRide data stream
    val rides = env
      .addSource(
      new FlinkKafkaConsumer082[TaxiRide](
        RideCleansingToKafka.CLEANSED_RIDES_TOPIC,
        new TaxiRideSchema,
        kafkaProps))
      .assignTimestamps(new TimestampExtractor[TaxiRide] {

        var curWatermark = 0L

        override def extractTimestamp(ride: TaxiRide, curTime: Long): Long = ride.time.getMillis

        override def extractWatermark(ride: TaxiRide, curTime: Long): Long = {
          curWatermark = curTime - (maxDelay * 1000)
          -1
        }

        override def getCurrentWatermark: Long = curWatermark
    })

    val rideSpeeds = rides
      // group records by rideId
      .keyBy("rideId")
      // compute the average speed of a ride
      .flatMap(new SpeedComputer)

    // emit the result on stdout
    rideSpeeds.print()

    // run the transformation pipeline
    env.execute("Average Ride Speed")
  }

  /**
   * Computes the average speed of a taxi ride from its start and end record.
   */
  class SpeedComputer extends RichFlatMapFunction[TaxiRide, (Long, Float)] {

    override def flatMap(ride: TaxiRide, out: Collector[(Long, Float)]): Unit = {
      val state = getRuntimeContext.getKeyValueState("ride", classOf[TaxiRide], null)

      if(state.value() == null) {
        // first ride
        state.update(ride)
      }
      else {
        // second ride
        val startEvent = if (ride.isStart) ride else state.value()
        val endEvent = if (ride.isStart) state.value() else ride

        val timeDiff = endEvent.time.getMillis - startEvent.time.getMillis
        val speed = if (timeDiff != 0) {
          (endEvent.travelDistance / timeDiff) * 60 * 60 * 1000
        } else {
          -1
        }

        out.collect( (startEvent.rideId, speed) )
      }
    }
  }

}

