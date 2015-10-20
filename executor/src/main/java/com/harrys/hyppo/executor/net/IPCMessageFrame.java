package com.harrys.hyppo.executor.net;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.CRC32;

/**
 * Created by jpetty on 7/9/15.
 */
public final class IPCMessageFrame {

    public static final int     PacketSizeOffset    = 0;
    public static final int     PacketSizeLength    = Integer.BYTES;
    public static final int     ChecksumOffset      = (PacketSizeOffset + PacketSizeLength);
    public static final int     ChecksumLength      = Long.BYTES;
    public static final int     HeaderSize          = (PacketSizeLength + ChecksumLength);
    public static final int     ContentOffset       = HeaderSize;
    public static final byte[]  TerminationSequence = new byte[HeaderSize];
    public static final int     PacketOverheadSize  = (HeaderSize + TerminationSequence.length);

    //  Pre-allocate Base64 encoder and decoder
    private static final Base64.Encoder B64Encoder  = Base64.getEncoder();
    private static final Base64.Decoder B64Decoder  = Base64.getDecoder();

    //  Instance member declarations
    private final byte[] content;

    /**
     * Private constructor to force using {@link #createFromContent(byte[])} or {@link #createFromWireFormat(byte[])}
     * @param content The literal byte contents of the message (not the wire format)
     */
    private IPCMessageFrame(final byte[] content){
        if (content.length == 0){
            throw new IllegalArgumentException("contents must be at least 1 byte long");
        }
        this.content  = content;
    }

    /**
     * @return The size of the underlying content in bytes
     */
    public final int getContentLength(){
        return content.length;
    }

    /**
     * @return The literal byte contents
     */
    public final byte[] getContent() {
        return content;
    }

    /**
     * @return A transmission ready packet including the Header (length + CRC value), escaped content, and termination sequence
     */
    public final byte[] toWireFormat(){
        final byte[] escapedBytes = B64Encoder.encode(this.content);
        final int packetSize      = escapedBytes.length + HeaderSize + TerminationSequence.length;
        final ByteBuffer buffer   = ByteBuffer.allocate(packetSize);
        buffer.putInt(packetSize);
        buffer.putLong(computeChecksum(escapedBytes));
        buffer.put(escapedBytes);
        buffer.put(TerminationSequence);
        return buffer.array();
    }

    /**
     * Decodes a message frame from the wire format by validating the packet length, header fields, and presence of
     * the correct termination sequence
     * @param packet The bytes from the wire format
     * @return The decoded message contents
     * @throws InvalidMessageFrameException if the bytes passed do not represent a valid {@link IPCMessageFrame}
     */
    public static final IPCMessageFrame createFromWireFormat(final byte[] packet) throws InvalidMessageFrameException {
        if (packet.length <= PacketOverheadSize){
            throw new InvalidMessageFrameException("Minimum possible packet size is " + (PacketOverheadSize + 1) + ". Received: " + packet.length);
        }
        final ByteBuffer reader = ByteBuffer.wrap(packet);
        final int declaredSize  = reader.getInt();
        if (declaredSize != packet.length){
            throw new InvalidMessageFrameException("Declared packet size does not match actual size. Packet claimed " + declaredSize + ". Actual size: " + packet.length + ". Content: " + DatatypeConverter.printHexBinary(packet));
        }

        final long declaredCrc = reader.getLong();
        final byte[] contents  = new byte[declaredSize - (PacketOverheadSize)];
        final byte[] trailing  = new byte[TerminationSequence.length];
        reader.get(contents, reader.arrayOffset(), contents.length);
        reader.get(trailing, reader.arrayOffset(), trailing.length);

        if (computeChecksum(contents) != declaredCrc){
            throw new InvalidMessageFrameException("Declared CRC does not match computed value. Declared: " + declaredCrc + " Actual CRC: " + computeChecksum(contents));
        }

        if (!Arrays.equals(trailing, TerminationSequence)){
            throw new InvalidMessageFrameException("Expected Terminating Sequence: " + DatatypeConverter.printHexBinary(TerminationSequence) + " Actual: " + DatatypeConverter.printHexBinary(trailing));
        }

        final byte[] decodedBytes = B64Decoder.decode(contents);

        return new IPCMessageFrame(decodedBytes);
    }

    /**
     * Creates a new {@link IPCMessageFrame} that wraps the provided byte array
     * @param content The bytes to wrap into a frame
     * @return the new {@link IPCMessageFrame} instance
     */
    public static final IPCMessageFrame createFromContent(final byte[] content){
        if (content.length == 0){
            throw new IllegalArgumentException("contents must be at least 1 byte long");
        }
        final byte[] copy = new byte[content.length];
        System.arraycopy(content, 0, copy, 0, content.length);
        return new IPCMessageFrame(copy);
    }

    /**
     * Computes the CRC32 checksum value for a given byte array
     * @param content The bytes to compute a CRC32 for
     * @return the CRC32 checksum value
     */
    private static final long computeChecksum(final byte[] content){
        if (content.length == 0){
            throw new IllegalArgumentException("contents must be at least 1 byte long");
        }
        final CRC32 check = new CRC32();
        check.update(content);
        return check.getValue();
    }
}
