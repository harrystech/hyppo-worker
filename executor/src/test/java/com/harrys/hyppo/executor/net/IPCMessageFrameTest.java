package com.harrys.hyppo.executor.net;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.net.*;
import java.util.Random;

/**
 * Created by jpetty on 7/10/15.
 */
public class IPCMessageFrameTest {

    @Test
    public void messagesShouldEncodeAndDecodeToTheSameValue() throws Exception {
        IPCMessageFrame original = IPCMessageFrame.createFromContent("some message".getBytes("UTF-8"));
        IPCMessageFrame decoded  = IPCMessageFrame.createFromWireFormat(original.toWireFormat());
        Assert.assertArrayEquals(decoded.getContent(), original.getContent());
    }


    @Test
    public void messagesShouldEncodeAndDecodeInAllSizes() throws Exception {
        final Random rand = new Random();
        for (int i = 1; i <= 2048; i++){
            final byte[] testBytes = new byte[i];
            rand.nextBytes(testBytes);

            final IPCMessageFrame original = IPCMessageFrame.createFromContent(testBytes);
            final IPCMessageFrame decoded  = IPCMessageFrame.createFromWireFormat(original.toWireFormat());
            Assert.assertArrayEquals(original.getContent(), decoded.getContent());
        }
    }


    @Test
    public void messagesShouldDecodeOverTheSocket() throws Exception {
        ServerSocket server = null;
        Socket client = null;

        final IPCMessageFrame message = IPCMessageFrame.createFromContent("message contents".getBytes());
        try {
            server = new ServerSocket(9191, 0, InetAddress.getLoopbackAddress());
            sendMessageFromServer(server, message);

            final IPCMessageFrame received = readMessageFromServer(new Socket(server.getInetAddress(), server.getLocalPort()));

            Assert.assertArrayEquals(received.getContent(), message.getContent());

        } finally {
            IOUtils.closeQuietly(server);
            IOUtils.closeQuietly(client);
        }
    }

    public IPCMessageFrame readMessageFromServer(final Socket client) throws Exception {
        InputStream input = client.getInputStream();
        byte[] readBytes  = IOUtils.toByteArray(input);
        client.close();
        return IPCMessageFrame.createFromWireFormat(readBytes);
    }

    public void sendMessageFromServer(final ServerSocket server, final IPCMessageFrame message) throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Socket inbound = null;
                try {
                    inbound = server.accept();
                    inbound.getOutputStream().write(message.toWireFormat());
                } catch (Exception e){
                    Assert.fail(e.getMessage());
                } finally {
                    IOUtils.closeQuietly(inbound);
                    IOUtils.closeQuietly(server);
                }
            }
        }).start();
    }
}
