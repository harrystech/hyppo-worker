package com.harrys.hyppo.executor.net;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.util.Arrays;

/**
 * An efficient stream searching class based on the Knuth-Morris-Pratt algorithm.
 * For more on the algorithm works see: http://www.inf.fh-flensburg.de/lang/algorithmen/pattern/kmpen.htm.
 */
public final class MessageFrameBuffer {
    private final byte[] pattern;
    private final int[] borders;

    private final int maxSize;

    private final InputStream stream;

    public MessageFrameBuffer(final int maxSize, final InputStream stream) {
        this.maxSize = maxSize;
        this.stream  = stream;
        this.pattern = Arrays.copyOf(IPCMessageFrame.TerminationSequence, IPCMessageFrame.TerminationSequence.length);
        this.borders = new int[pattern.length + 1];
        preProcess();
    }

    public final IPCMessageFrame read() throws IOException, InvalidMessageFrameException {
        final byte[] result = this.search(this.stream);
        if (result == null) {
            throw new IOException("Failed to find the next message in stream!");
        }
        return IPCMessageFrame.createFromWireFormat(result);
    }

    /**
     * Searches for the next occurrence of the pattern in the stream, starting from the current stream position. Note
     * that the position of the stream is changed. If a match is found, the stream points to the end of the match -- i.e. the
     * byte AFTER the pattern. Else, the stream is entirely consumed. The latter is because InputStream semantics make it difficult to have
     * another reasonable default, i.e. leave the stream unchanged.
     *
     * @return bytes consumed if found, -1 otherwise.
     * @throws IOException
     */
    private final byte[] search(final InputStream stream) throws IOException {
        int bytesRead = 0;

        int b;
        int j = 0;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        while ((b = stream.read()) != -1) {
            bytesRead++;

            baos.write(b);

            while (j >= 0 && (byte)b != pattern[j]) {
                j = borders[j];
            }
            // Move to the next character in the pattern.
            ++j;

            // If we've matched up to the full pattern length, we found it.  Return,
            // which will automatically save our position in the InputStream at the point immediately
            // following the pattern match.
            if (j == pattern.length) {
                return baos.toByteArray();
            } else if (bytesRead >= this.maxSize){
                throw new BufferOverflowException();
            }
        }

        // No dice, Note that the stream is now completely consumed.
        return null;
    }

    /**
     * Builds up a table of longest "borders" for each prefix of the pattern to find. This table is stored internally
     * and aids in implementation of the Knuth-Moore-Pratt string search.
     * <p>
     * For more information, see: http://www.inf.fh-flensburg.de/lang/algorithmen/pattern/kmpen.htm.
     */
    private final void preProcess() {
        int i = 0;
        int j = -1;
        borders[i] = j;
        while (i < pattern.length) {
            while (j >= 0 && pattern[i] != pattern[j]) {
                j = borders[j];
            }
            borders[++i] = ++j;
        }
    }
}
