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

package com.dataartisans.flinktraining.exercises.datastream_java.popular_places;

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.ride_cleansing.RideCleansing;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Java reference implementation for the "Popular Places" exercise of the Flink training
 * (http://dataartisans.github.io/flink-training).
 *
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class PopularPlaces {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		final int popThreshold = 20; // threshold for popular places
		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final float servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		// find n most popular spots
		DataStream<Tuple4<Float, Float, Boolean, Integer>> popularSpots = rides
				// remove all rides which are not within NYC
				.filter(new RideCleansing.NYCFilter())
				// match ride to grid cell and event type (start or end)
				.map(new GridCellMatcher())
				// partition by cell id and event type
				.keyBy(0, 1)
				// build sliding window
				.timeWindow(Time.minutes(15), Time.minutes(5))
				// count ride events in window
				.reduce(new RideCounter())
				// filter by popularity threshold
				.filter(new FilterFunction<Tuple3<Integer, Boolean, Integer>>() {
					@Override
					public boolean filter(Tuple3<Integer, Boolean, Integer> count) throws Exception {
						return count.f2 >= popThreshold;
					}
				})
				// map grid cell to coordinates
				.map(new GridToCoordinates());

		// print result on stdout
		popularSpots.print();

		// execute the transformation pipeline
		env.execute("Popular Places");
	}

	/**
	 * Map taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple3<Integer, Boolean, Integer>> {

		@Override
		public Tuple3<Integer, Boolean, Integer> map(TaxiRide taxiRide) throws Exception {
			if(taxiRide.isStart) {
				// get grid cell id for start location
				int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
				return new Tuple3<Integer, Boolean, Integer>(gridId, true, 1);
			} else {
				// get grid cell id for end location
				int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
				return new Tuple3<Integer, Boolean, Integer>(gridId, false, 1);
			}
		}
	}

	/**
	 * Counts the number of rides arriving or departing.
	 */
	public static class RideCounter implements ReduceFunction<Tuple3<Integer, Boolean, Integer>> {

		@Override
		public Tuple3<Integer, Boolean, Integer> reduce(
				Tuple3<Integer, Boolean, Integer> r1,
				Tuple3<Integer, Boolean, Integer> r2) throws Exception {

			// add counts
			r1.f2 += r2.f2;
			return r1;
		}
	}

	/**
	 * Maps the grid cell id back to longitude and latitude coordinates.
	 */
	public static class GridToCoordinates implements
			MapFunction<Tuple3<Integer, Boolean, Integer>, Tuple4<Float, Float, Boolean, Integer>> {

		@Override
		public Tuple4<Float, Float, Boolean, Integer> map(Tuple3<Integer, Boolean, Integer> cellCount) throws Exception {

			return new Tuple4<Float, Float, Boolean, Integer>(
					GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0),
					cellCount.f1,
					cellCount.f2);
		}
	}

}
