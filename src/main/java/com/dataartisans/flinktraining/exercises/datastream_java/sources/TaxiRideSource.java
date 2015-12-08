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

package com.dataartisans.flinktraining.exercises.datastream_java.sources;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;

/**
 * This SourceFunction generates a data stream of TaxiRide records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 *
 * In order to simulate a realistic stream source, the SourceFunction serves events proportional to
 * their timestamps. In addition, the serving of events can be delayed by a bounded random delay
 * which causes the events to be served slightly out-of-order of their timestamps.
 *
 * The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 *
 * This SourceFunction is an EventSourceFunction and does continuously emit watermarks.
 * Hence it can only operate in event time mode which is configured as follows:
 *
 *   StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 *
 */
public class TaxiRideSource implements EventTimeSourceFunction<TaxiRide> {

	private final int maxDelayMsecs;
	private final int watermarkDelayMSecs;

	private final String dataFilePath;
	private final float servingSpeed;

	private transient BufferedReader reader;
	private transient InputStream gzipStream;

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * at the speed at which they were originally generated.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
	 */
	public TaxiRideSource(String dataFilePath) {
		this(dataFilePath, 0, 1.0f);
	}

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served out of their time stamp order with specified maximum random delay
	 * at the speed at which they were originally generated.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
	 * @param maxEventDelaySecs The max time in seconds by which events are delayed.
	 */
	public TaxiRideSource(String dataFilePath, int maxEventDelaySecs) {
		this(dataFilePath, maxEventDelaySecs, 1.0f);
	}

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served exactly in order of their time stamps
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiRideSource(String dataFilePath, float servingSpeedFactor) {
		this(dataFilePath, 0, servingSpeedFactor);
	}

	/**
	 * Serves the TaxiRide records from the specified and ordered gzipped input file.
	 * Rides are served out-of time stamp order with specified maximum random delay
	 * in a serving speed which is proportional to the specified serving speed factor.
	 *
	 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
	 * @param maxEventDelaySecs The max time in seconds by which events are delayed.
	 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
	 */
	public TaxiRideSource(String dataFilePath, int maxEventDelaySecs, float servingSpeedFactor) {
		if(maxEventDelaySecs < 0) {
			throw new IllegalArgumentException("Max event delay must be positive");
		}
		this.dataFilePath = dataFilePath;
		this.maxDelayMsecs = maxEventDelaySecs * 1000;
		this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
		this.servingSpeed = servingSpeedFactor;
	}

	@Override
	public void run(SourceContext<TaxiRide> sourceContext) throws Exception {

		gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
		reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

		if(this.maxDelayMsecs == 0) {
			// simple case
			generateOrderedStream(sourceContext);
		}
		else {
			generateUnorderedStream(sourceContext);
		}

		this.reader.close();
		this.reader = null;
		this.gzipStream.close();
		this.gzipStream = null;

	}

	private void generateOrderedStream(SourceContext<TaxiRide> sourceContext) throws Exception {

		long servingStartTime = Calendar.getInstance().getTimeInMillis();
		long dataStartTime;

		long nextWatermark;
		long nextWatermarkServingTime;

		String line;
		if (reader.ready() && (line = reader.readLine()) != null) {
			// read first ride
			TaxiRide ride = TaxiRide.fromString(line);
			// extract starting timestamp
			dataStartTime = ride.time.getMillis();
			// schedule next watermark
			nextWatermark = dataStartTime + (watermarkDelayMSecs);
			nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark);
			// emit first ride
			sourceContext.collectWithTimestamp(ride, ride.time.getMillis());
		} else {
			return;
		}

		// emit all subsequent rides proportial to their timestamp and servingSpeed
		while (reader.ready() && (line = reader.readLine()) != null) {

			TaxiRide ride = TaxiRide.fromString(line);
			long eventTime = ride.time.getMillis();

			long now = Calendar.getInstance().getTimeInMillis();
			long eventServingTime = toServingTime(servingStartTime, dataStartTime, eventTime);
			long eventWait = eventServingTime - now;
			long watermarkWait = nextWatermarkServingTime - now;

			if(eventWait < watermarkWait) {
				Thread.sleep(eventWait > 0 ? eventWait : 0);
			}
			else if(eventWait > watermarkWait) {
				Thread.sleep(watermarkWait > 0 ? watermarkWait : 0);
				sourceContext.emitWatermark(new Watermark(nextWatermark));
				nextWatermark = nextWatermark + (watermarkDelayMSecs);
				nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark);
				long remainWait = eventWait - watermarkWait;
				Thread.sleep(remainWait > 0 ? remainWait : 0);
			}
			else if(eventWait == watermarkWait) {
				Thread.sleep(watermarkWait > 0 ? watermarkWait : 0);
				// -1 to ensure that no following events have the same timestamp
				sourceContext.emitWatermark(new Watermark(nextWatermark - 1));
				nextWatermark = nextWatermark + (watermarkDelayMSecs);
				nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark);
			}

			sourceContext.collectWithTimestamp(ride, ride.time.getMillis());
		}

	}

	private void generateUnorderedStream(SourceContext<TaxiRide> sourceContext) throws Exception {

		long servingStartTime = Calendar.getInstance().getTimeInMillis();
		long dataStartTime;

		Random rand = new Random(7452);
		PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<Tuple2<Long, Object>>(
				new Comparator<Tuple2<Long, Object>>() {
					@Override
					public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
						return o1.f0.compareTo(o2.f0);
					}
				});

		// read first ride and insert it into emit schedule
		String line;
		TaxiRide ride;
		if (reader.ready() && (line = reader.readLine()) != null) {
			// read first ride
			ride = TaxiRide.fromString(line);
			// extract starting timestamp
			dataStartTime = ride.time.getMillis();
			// get delayed time
			long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);
			emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));
			// schedule next watermark
			long watermarkTime = dataStartTime + watermarkDelayMSecs;
			Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
			emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));

		} else {
			return;
		}

		// peek at next ride
		if (reader.ready() && (line = reader.readLine()) != null) {
			ride = TaxiRide.fromString(line);
		}

		// read rides one-by-one and emit a random ride from the buffer each time
		while (emitSchedule.size() > 0 || reader.ready()) {

			// insert all events into schedule that might be emitted next
			long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
			long rideEventTime = ride != null ? ride.time.getMillis() : -1;
			while(
					ride != null && ( // while there is a ride AND
						emitSchedule.isEmpty() || // and no ride in schedule OR
						rideEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough rides in schedule
					)
			{
				// insert event into emit schedule
				long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
				emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

				// read next ride
				if (reader.ready() && (line = reader.readLine()) != null) {
					ride = TaxiRide.fromString(line);
					rideEventTime = ride.time.getMillis();
				}
				else {
					ride = null;
					rideEventTime = -1;
				}
			}

			// emit schedule is updated, emit next element in schedule
			Tuple2<Long, Object> head = emitSchedule.poll();
			long delayedEventTime = head.f0;

			long now = Calendar.getInstance().getTimeInMillis();
			long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
			long waitTime = servingTime - now;

			Thread.sleep( (waitTime > 0) ? waitTime : 0);

			if(head.f1 instanceof TaxiRide) {
				TaxiRide emitRide = (TaxiRide)head.f1;
				// emit ride
				sourceContext.collectWithTimestamp(emitRide, emitRide.time.getMillis());
			}
			else if(head.f1 instanceof Watermark) {
				Watermark emitWatermark = (Watermark)head.f1;
				// emit watermark
				sourceContext.emitWatermark(emitWatermark);
				// schedule next watermark
				long watermarkTime = delayedEventTime + watermarkDelayMSecs;
				Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
				emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
			}
		}
	}

	public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
		long dataDiff = eventTime - dataStartTime;
		return servingStartTime + (long)(dataDiff / this.servingSpeed);
	}

	public long getNormalDelayMsecs(Random rand) {
		long delay = -1;
		long x = maxDelayMsecs / 2;
		while(delay < 0 || delay > maxDelayMsecs) {
			delay = (long)(rand.nextGaussian() * x) + x;
		}
		return delay;
	}

	@Override
	public void cancel() {
		try {
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.gzipStream != null) {
				this.gzipStream.close();
			}
		} catch(IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.gzipStream = null;
		}
	}

}

