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

package com.dataArtisans.flinkTraining.exercises.dataStreamScala.popularPlaces

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.sources.TaxiRideSource
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.GeoUtils
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * Scala reference implementation for the "Popular Places" exercise of the Flink training (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides arrived or departed in the last 15 minutes.
 *
 * Parameters:
 * --input path-to-input-directory
 * -- popThreshold min-num-of-taxis-for-popular-places
 * --speed serving-speed-of-generator
 *
 */
object PopularPlaces {
  private val COUNT_WINDOW_LENGTH = 15 * 60 * 1000L // 15 minutes in msecs
  private val COUNT_WINDOW_FREQUENCY = 5 * 60 * 1000L // 5 minutes in msecs

  def main(args: Array[String]) {

    // read parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")
    val popThreashold = params.getInt("popThreshold")
    val servingSpeedFactor = params.getFloat("speed", 1.0f)

    // adjust window size and eviction interval to fast-forward factor
    val windowSize = (COUNT_WINDOW_LENGTH / servingSpeedFactor).toLong
    val evictionInterval = (COUNT_WINDOW_FREQUENCY / servingSpeedFactor).toLong

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // start the data generator
    val rides = env.addSource(new TaxiRideSource(input, servingSpeedFactor))

//    // find n most popular spots
//    val popularSpots = rides
//      // remove all rides which are not within NYC
//      .filter { r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat) }
//      // match ride to grid cell and event type (start or end)
//      .map(new GridCellMatcher)
//      // partition by cell id and event type
//      .groupBy(0, 1)
//      // build sliding window
//      .window(Time.of(windowSize, TimeUnit.MILLISECONDS))
//        .every(Time.of(evictionInterval, TimeUnit.MILLISECONDS))
//      // count events in window
//      .mapWindow(new PopularityCounter(popThreashold))
//      .flatten()
//      // map grid cell to coordinates
//      .map(new GridToCoordinates)
//
//    // print result on stdout
//    popularSpots.print()

    // execute the transformation pipeline
    env.execute("Popular Places")
  }

  /**
   * Map taxi ride to grid cell and event type.
   * Start records use departure location, end record use arrival location.
   */
  class GridCellMatcher extends MapFunction[TaxiRide, (Int, Boolean)] {

    def map(taxiRide: TaxiRide): (Int, Boolean) = {
      if (taxiRide.isStart) {
        // get grid cell id for start location
        val gridId: Int = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat)
        (gridId, true)
      } else {
        // get grid cell id for end location
        val gridId: Int = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat)
        (gridId, false)
      }
    }
  }

  /**
   * Count window events for grid cell and event type.
   * Only emits records if the count is equal or larger than the popularity threshold.
   */
//  class PopularityCounter(popThreshold: Int) extends WindowMapFunction[(Int, Boolean), (Int, Boolean, Int)] {
//
//    def mapWindow(values: java.lang.Iterable[(Int, Boolean)], out: Collector[(Int, Boolean, Int)]) {
//
//      // count records in window and build output record
//      val result = values.asScala.foldLeft((0, false, 0)) { (l, r) => (r._1, r._2, l._3 + 1) }
//
//      // check threshold
//      if (result._3 > popThreshold) {
//        // emit record
//        out.collect(result)
//      }
//    }
//  }

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

