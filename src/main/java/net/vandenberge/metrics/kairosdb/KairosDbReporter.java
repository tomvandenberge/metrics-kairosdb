package net.vandenberge.metrics.kairosdb;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

/**
 * A reporter which publishes metric values to a KairosDB server.
 * 
 * @see <a href="https://code.google.com/p/kairosdb/">KairosDB - Fast scalable
 *      time series database</a>
 */
public class KairosDbReporter extends AbstractPollingReporter implements MetricProcessor<KairosDbReporter.Context> {

	private static final Pattern TAG_PATTERN = Pattern.compile("[\\p{Alnum}\\.\\-_/]+");
	private static final Logger LOGGER = LoggerFactory.getLogger(KairosDbReporter.class);

	private final KairosDbClient client;
	private final Clock clock;
	private final String prefix;
	private final MetricPredicate predicate;

	public static class Context {
		private long time;

		private Context(long time) {
			this.time = time;
		}
	}

	/**
	 * Returns a new {@link Builder} for {@link KairosDbReporter}.
	 * 
	 * @param registry
	 *            the registry to report
	 * @return a {@link Builder} instance for a {@link KairosDbReporter}
	 */
	public static Builder forRegistry(MetricsRegistry registry) {
		return new Builder(registry);
	}

	/**
	 * A builder for {@link KairosDbReporter} instances. Defaults to not using a
	 * prefix, using the default clock, converting rates to events/second,
	 * converting durations to milliseconds, and not filtering metrics.
	 */
	public static class Builder {
		private final MetricsRegistry registry;
		private Clock clock;
		private String prefix;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;
		private MetricPredicate filter;
		private Map<String, String> tags;

		private Builder(MetricsRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricPredicate.ALL;
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
		 * Add a tag to each submitted metric. Both tag name and value must
		 * match the following regular expression:
		 * 
		 * <pre>
		 * [\p{Alnum}\.\-_/]+
		 * </pre>
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

		public Builder withTags(Map<String, String> tags) {
			for (Entry<String, String> tag : tags.entrySet()) {
				validateTag(tag.getKey(), tag.getValue());
			}
			this.tags.putAll(tags);
			return this;
		}

		/**
		 * Only report metrics which match the given filter.
		 * 
		 * @param filter
		 *            a {@link MetricFilter}
		 * @return {@code this}
		 */
		public Builder filter(MetricPredicate filter) {
			this.filter = filter;
			return this;
		}

		/**
		 * Builds a {@link KairosDbReporter} with the given properties, sending
		 * metrics using the given {@link KairosDbClient} client.
		 * 
		 * @param kairosDb
		 *            a {@link KairosDb} client
		 * @return a {@link KairosDbReporter}
		 */
		public KairosDbReporter build(KairosDbClient kairosDb) {
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

	private KairosDbReporter(MetricsRegistry registry, KairosDbClient kairosDb, Clock clock, String prefix,
			TimeUnit rateUnit, TimeUnit durationUnit, MetricPredicate predicate) {
		super(registry, "kairosdb-reporter");
		this.client = kairosDb;
		this.clock = clock;
		this.prefix = prefix;
		this.predicate = predicate;
	}

	public void processTimer(MetricName metricName, Timer timer, Context context) throws Exception {
		processMeter(metricName, timer, context);
		final Snapshot snapshot = timer.getSnapshot();
		client.send(name(metricName, "max"), format(timer.max()), context.time);
		client.send(name(metricName, "mean"), format(timer.mean()), context.time);
		client.send(name(metricName, "min"), format(timer.min()), context.time);
		client.send(name(metricName, "stddev"), format(timer.stdDev()), context.time);
		client.send(name(metricName, "p50"), format(snapshot.getMedian()), context.time);
		client.send(name(metricName, "p75"), format(snapshot.get75thPercentile()), context.time);
		client.send(name(metricName, "p95"), format(snapshot.get95thPercentile()), context.time);
		client.send(name(metricName, "p98"), format(snapshot.get98thPercentile()), context.time);
		client.send(name(metricName, "p99"), format(snapshot.get99thPercentile()), context.time);
		client.send(name(metricName, "p999"), format(snapshot.get999thPercentile()), context.time);
	}

	public void processMeter(MetricName metricName, Metered meter, Context context) throws Exception {
		client.send(name(metricName, "count"), format(meter.count()), context.time);
		client.send(name(metricName, "m1_rate"), format(meter.oneMinuteRate()), context.time);
		client.send(name(metricName, "m5_rate"), format(meter.fiveMinuteRate()), context.time);
		client.send(name(metricName, "m15_rate"), format(meter.fifteenMinuteRate()), context.time);
		client.send(name(metricName, "mean_rate"), format(meter.meanRate()), context.time);
	}

	public void processHistogram(MetricName metricName, Histogram histogram, Context context) throws Exception {
		final Snapshot snapshot = histogram.getSnapshot();
		client.send(name(metricName, "count"), format(histogram.count()), context.time);
		client.send(name(metricName, "max"), format(histogram.max()), context.time);
		client.send(name(metricName, "mean"), format(histogram.mean()), context.time);
		client.send(name(metricName, "min"), format(histogram.min()), context.time);
		client.send(name(metricName, "stddev"), format(histogram.stdDev()), context.time);
		client.send(name(metricName, "p50"), format(snapshot.getMedian()), context.time);
		client.send(name(metricName, "p75"), format(snapshot.get75thPercentile()), context.time);
		client.send(name(metricName, "p95"), format(snapshot.get95thPercentile()), context.time);
		client.send(name(metricName, "p98"), format(snapshot.get98thPercentile()), context.time);
		client.send(name(metricName, "p99"), format(snapshot.get99thPercentile()), context.time);
		client.send(name(metricName, "p999"), format(snapshot.get999thPercentile()), context.time);
	}

	public void processCounter(MetricName metricName, Counter counter, Context context) throws Exception {
		client.send(name(metricName, "count"), format(counter.count()), context.time);
	}

	public void processGauge(MetricName metricName, Gauge<?> gauge, Context context) throws Exception {
		final String value = format(gauge.value());
		if (value != null) {
			client.send(name(metricName, null), value, context.time);
		}
	}

	private String name(MetricName metricName, String suffix) {
		return name(prefix, metricName.getType(), metricName.getScope(), metricName.getName(), suffix);
	}

	private String format(Object o) {
		if (o instanceof Float) {
			return format(((Float) o).doubleValue());
		} else if (o instanceof Double) {
			return format(((Double) o).doubleValue());
		} else if (o instanceof Byte) {
			return format(((Byte) o).longValue());
		} else if (o instanceof Short) {
			return format(((Short) o).longValue());
		} else if (o instanceof Integer) {
			return format(((Integer) o).longValue());
		} else if (o instanceof Long) {
			return format(((Long) o).longValue());
		}
		return null;
	}

	private String format(long n) {
		return Long.toString(n);
	}

	private String format(double v) {
		return Double.toString(v);
	}

	@Override
	public void run() {
		LOGGER.debug("Starting KairosDbReporter run.");
		try {
			Set<Entry<String, SortedMap<MetricName, Metric>>> groupedMetrics = getMetricsRegistry().groupedMetrics(
					predicate).entrySet();
			for (Entry<String, SortedMap<MetricName, Metric>> entry : groupedMetrics) {
				for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
					subEntry.getValue().processWith(this, subEntry.getKey(), new Context(clock.time()));
				}
			}
		} catch (Exception e) {
			LOGGER.error("Error while reporting metrics to KairosDb", e);
		}
	}

	/**
	 * Concatenates elements to form a dotted name, eliding any null values or
	 * empty strings.
	 *
	 * @param name
	 *            the first element of the name
	 * @param names
	 *            the remaining elements of the name
	 * @return {@code name} and {@code names} concatenated by periods
	 */
	public static String name(String name, String... names) {
		final StringBuilder builder = new StringBuilder();
		append(builder, name);
		if (names != null) {
			for (String s : names) {
				append(builder, s);
			}
		}
		return builder.toString();
	}

	private static void append(StringBuilder builder, String part) {
		if (part != null && !part.isEmpty()) {
			if (builder.length() > 0) {
				builder.append('.');
			}
			builder.append(part);
		}
	}
}
