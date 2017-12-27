package net.vandenberge.metrics.kairosdb;

import net.vandenberge.metrics.kairosdb.KairosDb;

import org.fest.assertions.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import javax.net.SocketFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class KairosDbTest {
    private final SocketFactory socketFactory = mock(SocketFactory.class);
    private final InetSocketAddress address = new InetSocketAddress("example.com", 1234);
    private final KairosDb kairosDb = new KairosDb(address, socketFactory);

    private final Socket socket = mock(Socket.class);
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    @Before
    public void setUp() throws Exception {
        when(socket.getOutputStream()).thenReturn(output);

        when(socketFactory.createSocket(any(InetAddress.class),
                                        anyInt())).thenReturn(socket);

    }

    @Test
    public void connect() throws Exception {
        kairosDb.connect();

        verify(socketFactory).createSocket(address.getAddress(), address.getPort());
    }

    @Test
    public void disconnect() throws Exception {
        kairosDb.connect();
        kairosDb.close();

        verify(socket).close();
    }

    @Test
    public void doesNotAllowDoubleConnections() throws Exception {
        kairosDb.connect();
        try {
            kairosDb.connect();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage())
                    .isEqualTo("Already connected");
        }
    }

    @Test
    public void writesIntegerValue() throws Exception {
        kairosDb.connect();
        kairosDb.send("name", 333, 100);

        assertThat(output.toString())
                .isEqualTo("put name 100 333\n");
    }

    @Test
    public void writesDoubleValue() throws Exception {
	    	kairosDb.connect();
	    	kairosDb.send("name", 22.33, 100);
	    	
	    	assertThat(output.toString())
	    	.isEqualTo("put name 100 22.33\n");
    }

    /**
     * Infinity and Nan values are not sent. 
     */
    @Test
    public void writesPositiveInfinity() throws IOException {
	    	kairosDb.connect();
	    	kairosDb.send("name", Double.POSITIVE_INFINITY, 100);
	    	
	    	assertThat(output.toString()).isEqualTo("");
    }

    @Test
    public void writesNegativeInfinity() throws IOException {
	    	kairosDb.connect();
	    	kairosDb.send("name", Double.NEGATIVE_INFINITY, 100);
	    	
	    	assertThat(output.toString()).isEqualTo("");
    }

    @Test
    public void writesNaN() throws IOException {
	    	kairosDb.connect();
	    	kairosDb.send("name", Double.NaN, 100);
	    	
	    	assertThat(output.toString()).isEqualTo("");
    }
    
    @Test
    public void isFinite() {
    		assertThat(kairosDb.isFinite(Double.valueOf(123.123))).isTrue();
    		assertThat(kairosDb.isFinite(Float.valueOf(123.123f))).isTrue();
    		assertThat(kairosDb.isFinite(Double.MAX_VALUE)).isTrue();
    		assertThat(kairosDb.isFinite(Double.NaN)).isFalse();
    		assertThat(kairosDb.isFinite(Double.POSITIVE_INFINITY)).isFalse();
    		assertThat(kairosDb.isFinite(Double.NEGATIVE_INFINITY)).isFalse();
    }
    
    @Test
    public void sanitizesNames() throws Exception {
        kairosDb.connect();
        kairosDb.send("name woo", 42, 100);

        assertThat(output.toString())
                .isEqualTo("put name-woo 100 42\n");
    }
}
