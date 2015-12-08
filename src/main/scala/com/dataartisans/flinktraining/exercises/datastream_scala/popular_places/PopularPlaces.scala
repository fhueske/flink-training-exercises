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

package com.dataartisans.flinktraining.exercises.datastream_scala.popular_places

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.{EventTime, Time}

/**
 * Scala reference implementation for the "Popular Places" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 *
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
object PopularPlaces {

  def main(args: Array[String]) {

    // read parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val popThreshold = 20 // threshold for popular places
    val maxDelay = 60 // events are out of order by max 60 seconds
    val speed = 600 // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // start the data generator
    val rides = env.addSource(new TaxiRideSource(input, maxDelay, speed))

//    // find n most popular spots
    val popularSpots = rides
      // remove all rides which are not within NYC
      .filter { r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat) }
      // match ride to grid cell and event type (start or end)
      .map(new GridCellMatcher)
      // partition by cell id and event type
      .keyBy(0, 1)
      // build sliding window
      .timeWindow(Time.minutes(15), Time.minutes(5))
      // count events in window
      .reduce( (r1, r2) => (r1._1, r1._2, r1._3 + r2._3) )
      // filter by popularity threshold
      .filter( c => c._3 >= popThreshold )
      // map grid cell to coordinates
      .map(new GridToCoordinates)

    // print result on stdout
    popularSpots.print()

    // execute the transformation pipeline
    env.execute("Popular Places")
  }

  /**
   * Map taxi ride to grid cell and event type.
   * Start records use departure location, end record use arrival location.
   */
  class GridCellMatcher extends MapFunction[TaxiRide, (Int, Boolean, Int)] {

    def map(taxiRide: TaxiRide): (Int, Boolean, Int) = {
      if (taxiRide.isStart) {
        // get grid cell id for start location
        val gridId: Int = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat)
        (gridId, true, 1)
      } else {
        // get grid cell id for end location
        val gridId: Int = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat)
        (gridId, false, 1)
      }
    }
  }

  /**
   * Maps the grid cell id back to longitude and latitude coordinates.
   */
  class GridToCoordinates extends MapFunction[(Int, Boolean, Int), (Float, Float, Boolean, Int)] {

    def map(cellCount: (Int, Boolean, Int)): (Float, Float, Boolean, Int) = {
      val longitude = GeoUtils.getGridCellCenterLon(cellCount._1)
      val latitude = GeoUtils.getGridCellCenterLat(cellCount._1)
      (longitude, latitude, cellCount._2, cellCount._3)
    }
  }

}

