package net.vandenberge.metrics.kairosdb;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.net.SocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KairosDb implements KairosDbClient {

	private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");
	private static final Charset UTF_8 = Charset.forName("UTF-8");
	private static final Logger LOGGER = LoggerFactory.getLogger(KairosDb.class);

	private final InetSocketAddress address;
	private final SocketFactory socketFactory;
	private final Charset charset;

	private Socket socket;
	private Writer writer;
	private Map<String, String> tags = new LinkedHashMap<String, String>();

	/**
	 * Creates a new KairosDB client which connects to the given address using
	 * the default {@link SocketFactory}.
	 * 
	 * @param address
	 *            the address of the KairosDB server
	 */
	public KairosDb(InetSocketAddress address) {
		this(address, SocketFactory.getDefault());
	}

	/**
	 * Creates a new KairosDB client which connects to the given address and
	 * socket factory.
	 * 
	 * @param address
	 *            the address of the KairosDB server
	 * @param socketFactory
	 *            the socket factory
	 */
	public KairosDb(InetSocketAddress address, SocketFactory socketFactory) {
		this(address, socketFactory, UTF_8);
	}

	/**
	 * Creates a new KairosDB client which connects to the given address and
	 * socket factory using the given character set.
	 * 
	 * @param address
	 *            the address of the KairosDB server
	 * @param socketFactory
	 *            the socket factory
	 * @param charset
	 *            the character set used by the server
	 */
	public KairosDb(InetSocketAddress address, SocketFactory socketFactory, Charset charset) {
		this.address = address;
		this.socketFactory = socketFactory;
		this.charset = charset;
	}

	/**
	 * Connects to the KairosDB server.
	 * 
	 * @throws IllegalStateException
	 *             if the client is already connected.
	 * @throws IOException
	 *             if there is an error connecting.
	 * @return the name of the connected host and port.
	 */
	public String connect() throws IOException {
		if (socket != null) {
			throw new IllegalStateException("Already connected");
		}

		this.socket = socketFactory.createSocket(address.getAddress(), address.getPort());
		this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), charset));
		return address.toString();
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * Sends the given measurement to the server.
	 * 
	 * @param name
	 *            the name of the metric
	 * @param value
	 *            the value of the metric
	 * @param timestamp
	 *            the timestamp of the metric
	 * @throws IOException
	 *             if there was an error sending the metric
	 */
	public void send(String name, String value, long timestamp) throws IOException {
		StringBuilder builder = new StringBuilder();
		Writer writer = getWriter();
		builder.append("put ");
		builder.append(sanitize(name));
		builder.append(' ');
		builder.append(Long.toString(timestamp));
		builder.append(' ');
		builder.append(sanitize(value));
		for (Entry<String, String> entry : this.tags.entrySet()) {
			builder.append(' ');
			builder.append(entry.getKey());
			builder.append('=');
			builder.append(entry.getValue());
		}
		builder.append('\n');
		String message = builder.toString();
		writer.write(message);
		writer.flush();
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(message);
		}
	}

	public void close() throws IOException {
		if (socket != null) {
			socket.close();
		}
		this.socket = null;
		this.writer = null;
	}

	private String sanitize(String s) {
		return WHITESPACE.matcher(s).replaceAll("-");
	}

	private Writer getWriter() {
		if (writer == null) {
			throw new IllegalStateException("Not connected");
		}
		return writer;
	}
}
