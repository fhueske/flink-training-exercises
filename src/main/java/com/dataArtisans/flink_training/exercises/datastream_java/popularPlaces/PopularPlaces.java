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

package com.dataartisans.flink_training.exercises.datastream_java.popularplaces;

import com.dataartisans.flink_training.exercises.datastream_java.ridecleansing.RideCleansing;
import com.dataartisans.flink_training.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flink_training.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flink_training.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Java reference implementation for the "Popular Places" exercise of the Flink training (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides arrived or departed in the last 15 minutes.
 *
 * Parameters:
 *   -input path to input file
 *   -popThreshold minimum number of taxi rides for popular places
 *   -maxDelay maximum out of order delay of events
 *   -speed serving speed factor
 *
 */
public class PopularPlaces {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");
		final int popThreshold = Integer.parseInt(params.getRequired("popThreshold"));
		final int maxEventDelay = params.getInt("maxDelay", 0);
		final float servingSpeedFactor = params.getFloat("speed", 1.0f);

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
				// count events in window
				.fold(new Tuple3<Integer, Boolean, Integer>(0, false, 0),
						new FoldFunction<Tuple2<Integer, Boolean>, Tuple3<Integer, Boolean, Integer>>() {
					@Override
					public Tuple3<Integer, Boolean, Integer> fold(
							Tuple3<Integer, Boolean, Integer> cnt,
							Tuple2<Integer, Boolean> ride) throws Exception {

						cnt.f0 = ride.f0;
						cnt.f1 = ride.f1;
						cnt.f2++;
						return cnt;
					}
				})
				// filter by pop threshold
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
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

		@Override
		public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
			if(taxiRide.isStart) {
				// get grid cell id for start location
				int gridId = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
				return new Tuple2<Integer, Boolean>(gridId, true);
			} else {
				// get grid cell id for end location
				int gridId = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
				return new Tuple2<Integer, Boolean>(gridId, false);
			}
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
