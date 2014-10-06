package net.vandenberge.metrics.kairosdb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.vandenberge.metrics.kairosdb.KairosDbReporter.Builder;

import com.addthis.metrics.reporter.config.AbstractHostPortReporterConfig;
import com.addthis.metrics.reporter.config.HostPort;
import com.yammer.metrics.Metrics;

/**
 * This {@link com.addthis.metrics.reporter.config.ConfiguredReporter} is allows
 * to configure/instantiate a KairosDbReporter in a simple way. The
 * configuration settings are all bean properties, so an instance can easily be
 * created with yaml or spring.
 * 
 * @author Tom van den Berge
 */
public class KairosDbReporterConfig extends AbstractHostPortReporterConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(KairosDbReporterConfig.class);
	private KairosDbClient kairosDbClient;
	private Map<String, String> tags = new HashMap<String, String>();

	public void setKairosDbClient(KairosDbClient client) {
		this.kairosDbClient = client;
	}

	KairosDbClient getKairosDbClient() {
		return this.kairosDbClient;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = new HashMap<String, String>();
		for (Entry<String, String> tag : tags.entrySet()) {
			this.tags.put(tag.getKey(), getHostTagValue(tag.getValue()));
		}
	}

	@Override
	public boolean enable() {
		try {
			if (this.kairosDbClient == null) {
				this.kairosDbClient = createClient();
			}
			String connectInfo = this.kairosDbClient.connect();

			Builder builder = KairosDbReporter.forRegistry(Metrics.defaultRegistry()).filter(getMetricPredicate())
					.filter(getMetricPredicate());
			if (!this.tags.isEmpty()) {
				builder.withTags(this.tags);
			}
			KairosDbReporter reporter = builder.build(this.kairosDbClient);
			reporter.start(getPeriod(), getRealTimeunit());
			LOGGER.info("KairosDB reporter is started. Metrics will be sent to " + connectInfo);
			return true;
		} catch (NoHostConfiguredException e) {
			LOGGER.error(e.getMessage());
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
		return false;
	}

	private String getHostTagValue(String template) {
		setPrefix(template);
		return getResolvedPrefix();
	}

	private KairosDbClient createClient() throws NoHostConfiguredException {
		HostPort host = getConfiguredHost();
		if (host == null) {
			throw new NoHostConfiguredException();
		}

		return new KairosDb(new InetSocketAddress(host.getHost(), host.getPort()));
	}

	private HostPort getConfiguredHost() {
		List<HostPort> hosts = getHostListAndStringList();
		if (hosts == null || hosts.isEmpty()) {
			LOGGER.error("No hosts configured for KairosDb reporter");
			return null;
		}
		HostPort host = hosts.get(0);
		if (hosts.size() > 1) {
			LOGGER.warn("Multiple hosts specified. Only the first one is used: " + host.getHost() + ":"
					+ host.getPort());
		}
		return host;
	}

	@Override
	public List<HostPort> getFullHostList() {
		return getHostListAndStringList();
	}

	private static class NoHostConfiguredException extends Exception {
		private NoHostConfiguredException() {
			super("No host configured!");
		}
	}
}
