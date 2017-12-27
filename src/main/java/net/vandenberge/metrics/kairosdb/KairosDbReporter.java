package net.vandenberge.metrics.kairosdb;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * A reporter which publishes metric values to a KairosDB server.
 * 
 * @see <a href="https://code.google.com/p/kairosdb/">KairosDB - Fast scalable
 *      time series database</a>
 */
public class KairosDbReporter extends ScheduledReporter {

	private static final Pattern TAG_PATTERN = Pattern.compile("[\\p{Alnum}\\.\\-_/]+");
	private static final Logger LOGGER = LoggerFactory.getLogger(KairosDbReporter.class);

	private final KairosDb client;
	private final Clock clock;
	private final String prefix;

	/**
	 * Returns a new {@link Builder} for {@link KairosDbReporter}.
	 * 
	 * @param registry
	 *            the registry to report
	 * @return a {@link Builder} instance for a {@link KairosDbReporter}
	 */
	public static Builder forRegistry(MetricRegistry registry) {
		return new Builder(registry);
	}

	/**
	 * A builder for {@link KairosDbReporter} instances. Defaults to not using a
	 * prefix, using the default clock, converting rates to events/second,
	 * converting durations to milliseconds, and not filtering metrics.
	 */
	public static class Builder {
		private final MetricRegistry registry;
		private Clock clock;
		private String prefix;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;
		private MetricFilter filter;
		private Map<String, String> tags;

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
			this.tags = new LinkedHashMap<String, String>();
		}

		/**
		 * Use the given {@link Clock} instance for the time.
		 * 
		 * @param clock
		 *            a {@link Clock} instance
		 * @return {@code this}
		 */
		public Builder withClock(Clock clock) {
			this.clock = clock;
			return this;
		}

		/**
		 * Prefix all metric names with the given string.
		 * 
		 * @param prefix
		 *            the prefix for all metric names
		 * @return {@code this}
		 */
		public Builder prefixedWith(String prefix) {
			this.prefix = prefix;
			return this;
		}

		/**
		 * Convert rates to the given time unit.
		 * 
		 * @param rateUnit
		 *            a unit of time
		 * @return {@code this}
		 */
		public Builder convertRatesTo(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		/**
		 * Convert durations to the given time unit.
		 * 
		 * @param durationUnit
		 *            a unit of time
		 * @return {@code this}
		 */
		public Builder convertDurationsTo(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		/**
		 * Add a tag to each submitted metric. Both tag name and value must match the following regular expression:
		 * <pre>[\p{Alnum}\.\-_/]+</pre>
		 * 
		 * @param tagName
		 *            the tag name
		 * @param tagValue
		 *            the tag value
		 * @return {@code this}
		 */
		public Builder withTag(String tagName, String tagValue) {
			validateTag(tagName, tagValue);
			this.tags.put(tagName, tagValue);
			return this;
		}

		/**
		 * Only report metrics which match the given filter.
		 * 
		 * @param filter
		 *            a {@link MetricFilter}
		 * @return {@code this}
		 */
		public Builder filter(MetricFilter filter) {
			this.filter = filter;
			return this;
		}

		/**
		 * Builds a {@link KairosDbReporter} with the given properties, sending
		 * metrics using the given {@link Graphite} client.
		 * 
		 * @param kairosDb
		 *            a {@link KairosDb} client
		 * @return a {@link KairosDbReporter}
		 */
		public KairosDbReporter build(KairosDb kairosDb) {
			kairosDb.setTags(tags);
			return new KairosDbReporter(registry, kairosDb, clock, prefix, rateUnit, durationUnit, filter);
		}

		private void validateTag(String tagName, String tagValue) {
			validateTag(tagName);
			validateTag(tagValue);
		}

		static void validateTag(String tag) {
			if (tag == null || !TAG_PATTERN.matcher(tag).matches()) {
				throw new IllegalArgumentException(
						"\""
								+ tag
								+ "\" is not a valid tag name or value; it can only contain alphahumeric characters, period, slash, dash and underscore!");
			}
		}
	}

	private KairosDbReporter(MetricRegistry registry, KairosDb kairosDb, Clock clock, String prefix, TimeUnit rateUnit,
			TimeUnit durationUnit, MetricFilter filter) {
		super(registry, "kairosdb-reporter", filter, rateUnit, durationUnit);
		this.client = kairosDb;
		this.clock = clock;
		this.prefix = prefix;
	}

	@Override
	public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms,
			SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
		final long timestamp = clock.getTime();

		try {
			client.connect();

			for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
				reportGauge(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Counter> entry : counters.entrySet()) {
				reportCounter(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
				reportHistogram(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Meter> entry : meters.entrySet()) {
				reportMetered(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Timer> entry : timers.entrySet()) {
				reportTimer(entry.getKey(), entry.getValue(), timestamp);
			}
		} catch (IOException e) {
			LOGGER.warn("Unable to report to server {}", client);
		} finally {
			try {
				client.close();
			} catch (IOException e) {
				LOGGER.debug("Error disconnecting from server {}", client);
			}
		}
	}

	private void reportTimer(String name, Timer timer, long timestamp) throws IOException {
		final Snapshot snapshot = timer.getSnapshot();

		client.send(prefix(name, "max"), convertDuration(snapshot.getMax()), timestamp);
		client.send(prefix(name, "mean"), convertDuration(snapshot.getMean()), timestamp);
		client.send(prefix(name, "min"), convertDuration(snapshot.getMin()), timestamp);
		client.send(prefix(name, "stddev"), convertDuration(snapshot.getStdDev()), timestamp);
		client.send(prefix(name, "p50"), convertDuration(snapshot.getMedian()), timestamp);
		client.send(prefix(name, "p75"), convertDuration(snapshot.get75thPercentile()), timestamp);
		client.send(prefix(name, "p95"), convertDuration(snapshot.get95thPercentile()), timestamp);
		client.send(prefix(name, "p98"), convertDuration(snapshot.get98thPercentile()), timestamp);
		client.send(prefix(name, "p99"), convertDuration(snapshot.get99thPercentile()), timestamp);
		client.send(prefix(name, "p999"), convertDuration(snapshot.get999thPercentile()), timestamp);

		reportMetered(name, timer, timestamp);
	}

	private void reportMetered(String name, Metered meter, long timestamp) throws IOException {
		client.send(prefix(name, "count"), meter.getCount(), timestamp);
		client.send(prefix(name, "m1_rate"), convertRate(meter.getOneMinuteRate()), timestamp);
		client.send(prefix(name, "m5_rate"), convertRate(meter.getFiveMinuteRate()), timestamp);
		client.send(prefix(name, "m15_rate"), convertRate(meter.getFifteenMinuteRate()), timestamp);
		client.send(prefix(name, "mean_rate"), convertRate(meter.getMeanRate()), timestamp);
	}

	private void reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
		final Snapshot snapshot = histogram.getSnapshot();
		client.send(prefix(name, "count"), histogram.getCount(), timestamp);
		client.send(prefix(name, "max"), snapshot.getMax(), timestamp);
		client.send(prefix(name, "mean"), snapshot.getMean(), timestamp);
		client.send(prefix(name, "min"), snapshot.getMin(), timestamp);
		client.send(prefix(name, "stddev"), snapshot.getStdDev(), timestamp);
		client.send(prefix(name, "p50"), snapshot.getMedian(), timestamp);
		client.send(prefix(name, "p75"), snapshot.get75thPercentile(), timestamp);
		client.send(prefix(name, "p95"), snapshot.get95thPercentile(), timestamp);
		client.send(prefix(name, "p98"), snapshot.get98thPercentile(), timestamp);
		client.send(prefix(name, "p99"), snapshot.get99thPercentile(), timestamp);
		client.send(prefix(name, "p999"), snapshot.get999thPercentile(), timestamp);
	}

	private void reportCounter(String name, Counter counter, long timestamp) throws IOException {
		client.send(prefix(name, "count"), counter.getCount(), timestamp);
	}

	private void reportGauge(String name, Gauge<?> gauge, long timestamp) throws IOException {
		Object value = gauge.getValue();
		if (value instanceof Number) {
			client.send(prefix(name), (Number)value, timestamp);
		}
	}

	private String prefix(String... components) {
		return MetricRegistry.name(prefix, components);
	}
}
