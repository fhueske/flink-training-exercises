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

package com.dataartisans.flink_training.exercises.datastream_java.ridecleansing;

import com.dataartisans.flink_training.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flink_training.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flink_training.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Java reference implementation for the "Ride Cleansing" exercise of the Flink training (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that start and end within New York City.
 * The resulting stream should be printed.
 *
 * Parameters:
 *   -input path to input file
 *   -maxDelay maximum out of order delay of events
 *   -speed serving speed factor
 *
 */
public class RideCleansing {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.getRequired("input");
		final int maxEventDelay = params.getInt("maxDelay", 0);
		final float servingSpeedFactor = params.getFloat("speed", 1.0f);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(
				new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

		DataStream<TaxiRide> filteredRides = rides
				// filter out rides that do not start or stop in NYC
				.filter(new NYCFilter());

		// print the filtered stream
		filteredRides.print();

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
	}


	public static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}

}
