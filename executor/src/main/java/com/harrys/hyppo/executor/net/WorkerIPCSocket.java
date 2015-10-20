package com.harrys.hyppo.executor.net;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Created by jpetty on 7/10/15.
 */
public final class WorkerIPCSocket implements AutoCloseable, Closeable {
    //  Maximum possible message size
    public static final int BufferSize = 1024 * 1024 * 20;

    private final Socket socket;

    private final OutputStream output;

    private final MessageFrameBuffer buffer;

    private WorkerIPCSocket(final Socket connection) throws IOException {
        this.socket = connection;
        this.buffer = new MessageFrameBuffer(BufferSize, socket.getInputStream());
        this.output = socket.getOutputStream();
    }

    public final InetSocketAddress getLocalSocketAddress(){
        return (InetSocketAddress)this.socket.getLocalSocketAddress();
    }

    public final InetSocketAddress getRemoteSocketAddress(){
        return (InetSocketAddress)this.socket.getRemoteSocketAddress();
    }

    public final boolean isConnected(){
        return this.socket.isConnected();
    }

    public final boolean isClosed(){
        return this.socket.isClosed();
    }

    public final void sendFrame(final IPCMessageFrame message) throws IOException {
        if (this.socket.isClosed()){
            throw new IllegalStateException("Can't sendFrame message over closed socket!");
        }
        synchronized (output){
            output.write(message.toWireFormat());
            output.flush();
        }
    }

    public final IPCMessageFrame readFrame() throws IOException, InvalidMessageFrameException {
        if (this.socket.isClosed()){
            throw new IllegalStateException("Can't readFrame message over closed socket!");
        }
        synchronized (buffer){
            final IPCMessageFrame frame = buffer.read();
            return frame;
        }
    }

    @Override
    public final synchronized void close() throws IOException {
        this.socket.close();
    }

    public static final WorkerIPCSocket forClientSocket(final Socket clientSocket) throws IOException {
        if (!clientSocket.isConnected()){
            throw new IllegalArgumentException("Client connection must be fully established");
        }
        clientSocket.setTcpNoDelay(true);
        clientSocket.setKeepAlive(true);
        return new WorkerIPCSocket(clientSocket);
    }

    public static final WorkerIPCSocket connectToCommander(final int serverPort) throws IOException {
        return connectToCommander(serverPort, 0);
    }

    public static final WorkerIPCSocket connectToCommander(final int serverPort, final int connectTimeout) throws IOException {
        return connectToCommander(serverPort, connectTimeout, 0);
    }

    public static final WorkerIPCSocket connectToCommander(final int serverPort, final int connectTimeout, final int soTimeout) throws IOException {
        final Socket socket = new Socket();
        socket.setSoTimeout(soTimeout);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), serverPort), connectTimeout);
        return new WorkerIPCSocket(socket);
    }
}
