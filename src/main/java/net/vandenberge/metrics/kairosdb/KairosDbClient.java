package net.vandenberge.metrics.kairosdb;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * KairosDB transport client.
 * 
 * @author Tom van den Berge
 */
public interface KairosDbClient extends Closeable {

	/**
	 * Sets tags that will be sent with each metric.
	 */
	void setTags(Map<String, String> tags);

	/**
	 * Connects the client to the KairosDB server.
	 * 
	 * @return the host name and port of the connected KairosDB host. Useful for
	 *         logging purposes.
	 * @throws IOException
	 *             if the client could not connect to the host.
	 */
	String connect() throws IOException;

	/**
	 * Instructs the client to send a metric to the KairosDB host.
	 * 
	 * @param name
	 *            the name of the metric.
	 * @param value
	 *            the value of the metric.
	 * @param timestamp
	 *            the metric (ms).
	 * @throws IOException
	 *             if the client could not send the metric to the KairosDB host.
	 */
	void send(String name, String value, long timestamp) throws IOException;

	/**
	 * Closes the connection to the KairosDB host.
	 */
	void close() throws IOException;
}
