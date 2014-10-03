package net.vandenberge.metrics.kairosdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestKairosDbClient implements KairosDbClient {

	private Map<String, String> tags;
	private List<Metric> metrics = new ArrayList<>();

	@Override
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	@Override
	public String connect() throws IOException {
		return "<not connected to a real host>";
	}

	@Override
	public void send(String name, String value, long timestamp)
			throws IOException {
		this.metrics.add(new Metric(name, value, timestamp));
	}

	@Override
	public void close() throws IOException {

	}

	public List<Metric> getMetrics() {
		return metrics;
	}

	public static class Metric {
		private String name;
		private String value;
		private long timestamp;

		private Metric(String name, String value, long timestamp) {
			super();
			this.name = name;
			this.value = value;
			this.timestamp = timestamp;
		}

		public String getName() {
			return name;
		}

		public String getValue() {
			return value;
		}

		public long getTimestamp() {
			return timestamp;
		}

	}
}
