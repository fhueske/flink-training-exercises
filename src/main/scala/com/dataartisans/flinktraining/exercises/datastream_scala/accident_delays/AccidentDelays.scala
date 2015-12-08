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

package com.dataartisans.flinktraining.exercises.datastream_scala.accident_delays

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.{TaxiRide, Accident}
import com.dataartisans.flinktraining.exercises.datastream_java.sources.{AccidentSource, TaxiRideSource}
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Scala reference implementation for the "Accident Delays" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 *
 * The task of the exercise is to connect a data stream of taxi rides and a stream of accident
 * reports to identify taxi rides that might have been delayed due to accidents.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
object AccidentDelays {

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val maxDelay = 60 // events are out of order by max 60 seconds
    val speed = 600 // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // create taxi ride stream
    val rides = env
      .addSource(new TaxiRideSource(input, maxDelay, speed))
      // filter rides that do not start and end in NYC
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      // map taxi ride to all grid cells on its way
      .flatMap(new RouteCellMapper)

    // create accidents stream
    val accidents = env
      .addSource(new AccidentSource(speed))
      // map accident to grid cell
      .map(new AccidentCellMapper)

    val rideAccidents = rides
      // connect rides and accidents
      .connect(accidents)
      // key both streams on cell id
      .keyBy(0, 0)
      // match rides and accidents on the same grid cell
      .flatMap(new AccidentsPerRideCounter)

    rideAccidents.print()

    env.execute("Accident Delayed Rides")
  }

  /**
   * Maps an Accident to the grid cell id of its location.
   */
  class AccidentCellMapper extends MapFunction[Accident, (Int, Accident)] {

    def map(accident: Accident): (Int, Accident) = {
      val gridCell = GeoUtils.mapToGridCell(accident.lon, accident.lat)
      (gridCell, accident)
    }
  }

  /**
   * Maps a TaxiRide to all grid cells between its start and its end location.
   * For each grid cell on the way, a record is emitted.
   */
  class RouteCellMapper extends FlatMapFunction[TaxiRide, (Int, TaxiRide)] {

    def flatMap(taxiRide: TaxiRide, out: Collector[(Int, TaxiRide)]) {

      // get all grid cells on the way from start to end of the ride
      val routeCellIds = GeoUtils.mapToGridCellsOnWay(
        taxiRide.startLon,
        taxiRide.startLat,
        taxiRide.endLon,
        taxiRide.endLat).asScala

      // emit a record for each grid cell
      routeCellIds foreach { id => out.collect((id, taxiRide))}
    }
  }

  /**
   * Matches taxi rides which pass accidents on the same grid cell.
   * Accidents are kept until a clearance event is received.
   */
  class AccidentsPerRideCounter extends CoFlatMapFunction[(Int, TaxiRide), (Int, Accident), (Int, TaxiRide)] {

    // holds accidents indexed by cell id
    private val accidentsByCell = mutable.HashMap.empty[Int, mutable.Set[Long]]
    // holds taxi rides indexed by cell id
    private val ridesByCell = mutable.HashMap.empty[Int, mutable.Set[TaxiRide]]


    def flatMap1(ride: (Int, TaxiRide), out: Collector[(Int, TaxiRide)]) {
      // new ride event
      val cell = ride._1

      if (ride._2.isStart) {
        // ride event is a start event

        // check if an accident happened on the cell
        val accidents = accidentsByCell.getOrElseUpdate(cell, mutable.Set.empty[Long])
        if (accidents.nonEmpty) {
          // emit ride directly and do not remember it
          out.collect(ride)
        } else {
          // remember ride
          val set = ridesByCell.getOrElseUpdate(cell, mutable.Set.empty[TaxiRide])
          set.add(ride._2)
        }
      } else {
        // ride event is an end event

        // forget ride
        ridesByCell.get(cell) match {
          case Some(set) => set.remove(ride._2) // remove ride
          case _ => // we don't have the ride stored
        }
      }
    }

    def flatMap2(accident: (Int, Accident), out: Collector[(Int, TaxiRide)]) {
      // new accident event
      val cell = accident._1

      if (!accident._2.isCleared) {
        // accident event is an emergence event

        // check if taxi rides pass this cell
        ridesByCell.remove(cell) match {
          // emit all rides on cell
          case Some(set) => set foreach { ride => out.collect((cell, ride)) }
          case _ => // nothing to do
        }

        // remember accident
        val set = accidentsByCell.getOrElseUpdate(cell, mutable.Set.empty[Long])
        set.add(accident._2.accidentId)
      } else {
        // accident event is a clearance event

        // forget accident
        accidentsByCell.get(cell) match {
          case Some(set) => set.remove(accident._2.accidentId)
          case _ => // Nothing to do
        }
      }
    }
  }

}

