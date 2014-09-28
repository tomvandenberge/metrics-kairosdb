package net.vandenberge.metrics.kairosdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.vandenberge.metrics.kairosdb.TestKairosDbClient.Metric;

import org.junit.Assert;
import org.junit.Test;

import com.addthis.metrics.reporter.config.AbstractReporterConfig;
import com.addthis.metrics.reporter.config.ConfiguredReporter;
import com.addthis.metrics.reporter.config.ReporterConfig;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

public class KairosDbReporterTest {

	private KairosDb kairosDb = mock(KairosDb.class);
	private MetricsRegistry registry = new MetricsRegistry();
	private KairosDbReporter reporter = KairosDbReporter.forRegistry(registry)
			.build(kairosDb);

	/**
	 * Tags names and values can contain alphanumeric characters, slash, period,
	 * dash and underscore.
	 */
	@Test
	public void tags() {
		KairosDbReporter.Builder.validateTag("Valid");
		KairosDbReporter.Builder.validateTag("Valid.");
		KairosDbReporter.Builder.validateTag("Valid./");
		KairosDbReporter.Builder.validateTag("Valid./-");
		KairosDbReporter.Builder.validateTag("Valid./-__");
		KairosDbReporter.Builder.validateTag("_Va-//lid.");
	}

	@Test
	public void invalidTags() {
		assertInvalid(null);
		assertInvalid("");
		assertInvalid("invalid!");
		assertInvalid("{invalid}");
	}

	private static void assertInvalid(String tag) {
		try {
			KairosDbReporter.Builder.validateTag(tag);
			Assert.fail();
		} catch (IllegalArgumentException e) {
			// Expected
		}
	}

	@Test
	public void counter() throws IOException {
		Counter counter = registry.newCounter(this.getClass(), "counter");
		counter.inc(42);
		reporter.run();

		verify(kairosDb, times(1)).send(anyString(), anyString(), anyLong());
		verify(kairosDb).send(
				eq(getClass().getSimpleName() + ".counter.count"), eq("42"),
				anyLong());
	}

	@Test
	public void gauge() throws IOException {
		Gauge<Integer> gauge = new Gauge<Integer>() {
			@Override
			public Integer value() {
				return 42;
			}
		};
		registry.newGauge(this.getClass(), "gauge", gauge);
		reporter.run();

		verify(kairosDb, times(1)).send(anyString(), anyString(), anyLong());
		verify(kairosDb).send(eq(getClass().getSimpleName() + ".gauge"),
				eq("42"), anyLong());
	}

	@Test
	public void meter() throws IOException {
		Meter meter = registry.newMeter(this.getClass(), "meter", "eventtype",
				TimeUnit.MILLISECONDS);
		meter.mark(10);
		reporter.run();

		verify(kairosDb, times(5)).send(anyString(), anyString(), anyLong());
		verify(kairosDb).send(eq(getClass().getSimpleName() + ".meter.count"),
				eq("10"), anyLong());
		verify(kairosDb).send(
				eq(getClass().getSimpleName() + ".meter.mean_rate"),
				anyString(), anyLong());
		verify(kairosDb).send(
				eq(getClass().getSimpleName() + ".meter.m1_rate"), anyString(),
				anyLong());
		verify(kairosDb).send(
				eq(getClass().getSimpleName() + ".meter.m5_rate"), anyString(),
				anyLong());
		verify(kairosDb).send(
				eq(getClass().getSimpleName() + ".meter.m15_rate"),
				anyString(), anyLong());
	}

	@Test
	public void timer() throws IOException {
		Timer timer = registry.newTimer(this.getClass(), "timer");
		timer.update(10, TimeUnit.SECONDS);
		timer.update(2, TimeUnit.SECONDS);
		reporter.run();

		verify(kairosDb, times(15)).send(anyString(), anyString(), anyLong());
		verify(kairosDb).send(eq(getClass().getSimpleName() + ".timer.count"),
				eq("2"), anyLong());
		verify(kairosDb).send(eq(getClass().getSimpleName() + ".timer.mean"),
				eq("6000.0"), anyLong());
		verify(kairosDb).send(eq(getClass().getSimpleName() + ".timer.min"),
				eq("2000.0"), anyLong());
		verify(kairosDb).send(eq(getClass().getSimpleName() + ".timer.max"),
				eq("10000.0"), anyLong());
		verify(kairosDb).send(eq(getClass().getSimpleName() + ".timer.stddev"),
				eq("5656.85424949238"), anyLong());
	}

	@Test
	public void run() throws IOException, InterruptedException {

		ReporterConfig config = ReporterConfig
				.loadFromFile("src/test/resources/config.yaml");
		Counter counter = Metrics.newCounter(getClass(), "counter");
		Meter meter = Metrics.newMeter(getClass(), "meter", "foo",
				TimeUnit.SECONDS);
		config.enableAll();

		for (int i = 0; i < 10; i++) {
			counter.inc();
			meter.mark();
			Thread.sleep(105);
		}

		ConfiguredReporter reporterConfig = config.getReporters().get(0);
		assertTrue(reporterConfig instanceof KairosDbReporterConfig);
		KairosDbClient kairosDbClient = ((KairosDbReporterConfig)reporterConfig).getKairosDbClient();
		assertTrue(kairosDbClient instanceof TestKairosDbClient);
		TestKairosDbClient testKairosDbClient = (TestKairosDbClient)kairosDbClient;

		Map<String, String> expectedTags = new HashMap<>();
		expectedTags.put("host", "myhost");
		assertEquals(expectedTags, testKairosDbClient.getTags());
		List<Metric> metrics = testKairosDbClient.getMetrics();

		// Each run logs 6 metrics: 1 counter + 5 meter.
		assertEquals(60, metrics.size());
	}
}
