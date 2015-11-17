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

package com.dataartisans.flink_training.exercises.datastream_java.ridespeed;

import com.dataartisans.flink_training.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flink_training.exercises.datastream_java.ridecleansing.RideCleansing;
import com.dataartisans.flink_training.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Ride Speed" exercise of the Flink training (http://dataartisans.github.io/flink-training).
 * The task of the exercise is to compute the average speed of completed taxi rides from a data stream of taxi ride records.
 *
 * Parameters:
 *   -input path to input file
 *   -maxDelay maximum out of order delay of events
 *   -speed serving speed factor
 *
 */
public class RideSpeed {

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

		DataStream<Tuple2<Long, Float>> rideSpeeds = rides
				// filter out rides that do not start or stop in NYC
				.filter(new RideCleansing.NYCFilter())
				// group records by rideId
				.keyBy("rideId")
				// match ride start and end records
				.countWindow(2)
				// compute the average speed of a ride
				.apply(new SpeedComputer());

		// emit the result on stdout
		rideSpeeds.print();

		// run the transformation pipeline
		env.execute("Average Ride Speed");
	}

	/**
	 * Computes the average speed of a taxi ride from its start and end record.
	 */
	public static class SpeedComputer implements WindowFunction<TaxiRide, Tuple2<Long, Float>, Tuple, GlobalWindow> {

		private Tuple2<Long, Float> avgSpeed = new Tuple2<Long, Float>(0L, 0.0f);

		@SuppressWarnings("unchecked")
		@Override
		public void apply(
				Tuple key,
				GlobalWindow window,
				Iterable<TaxiRide> rides,
				Collector<Tuple2<Long, Float>> out) throws Exception
		{
			// extract key
			avgSpeed.f0 = ((Tuple1<Long>)key).f0;

			// identify start and end time and travel distance
			long startTime = 0;
			long endTime = 0;
			float distance = 0.0f;
			for(TaxiRide ride : rides) {
				if(ride.isStart) {
					startTime = ride.time.getMillis();
				}
				else {
					endTime = ride.time.getMillis();
					distance = ride.travelDistance;
				}
			}

			// compute average speed
			long timeDiff = endTime - startTime;
			if(timeDiff != 0) {
				// speed = distance / time
				avgSpeed.f1 = (distance / timeDiff) * (1000 * 60 * 60);
			}
			else {
				avgSpeed.f1 = -1f;
			}

			out.collect(avgSpeed);
		}
	}

}
