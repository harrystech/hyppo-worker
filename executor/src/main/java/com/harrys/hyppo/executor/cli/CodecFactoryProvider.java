package com.harrys.hyppo.executor.cli;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;

import java.util.Properties;

/**
 * Created by jpetty on 12/14/15.
 */
public final class CodecFactoryProvider {

    /**
     * The system property name to use when passing the codec via executor start options
     */
    public static final String CODEC_PROPERTY_NAME = "executor.avroFileCodec";

    /**
     * The name that corresponds to the default codec to use when no argument is passed
     */
    public static final String DEFAULT_CODEC_NAME  = DataFileConstants.SNAPPY_CODEC;

    /**
     * A local cached instance of the default codec
     */
    private static final CodecFactory defaultCodec = CodecFactory.fromString(DEFAULT_CODEC_NAME);

    /**
     * @return The {@link CodecFactory} to use when creating new avro files. This can be changed by passing the {@value #CODEC_PROPERTY_NAME} system property
     */
    public static final CodecFactory codecFactory(){
        final Properties props = System.getProperties();
        if (props.containsKey(CODEC_PROPERTY_NAME)){
            final String argument = props.getProperty(CODEC_PROPERTY_NAME);
            try {
                return CodecFactory.fromString(argument);
            } catch (AvroRuntimeException e){
                System.err.println("Unrecognized " + CODEC_PROPERTY_NAME + " value: " + argument + ". Using default codec: " + DEFAULT_CODEC_NAME);
                return CodecFactory.fromString(DEFAULT_CODEC_NAME);
            }
        } else {
            return defaultCodec;
        }
    }
}
