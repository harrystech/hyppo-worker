package com.harrys.hyppo.executor.util;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Created by jpetty on 11/9/15.
 */
public final class LocalDateTimeToJson {

    public static final class Serializer extends JsonSerializer<LocalDateTime> {
        @Override
        public void serialize(LocalDateTime value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            final Date legacy = (value == null) ? null : Date.from(value.toInstant(ZoneOffset.UTC));
            provider.defaultSerializeDateValue(legacy, jgen);
        }
    }

    public static final class Deserializer extends JsonDeserializer<LocalDateTime> {
        private static final ZoneId UTCZoneId = ZoneId.of(ZoneOffset.UTC.getId());
        @Override
        public LocalDateTime deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            final Date legacyValue = jp.getCodec().readValue(jp, Date.class);
            if (legacyValue == null){
                return null;
            } else {
                return LocalDateTime.ofInstant(legacyValue.toInstant(), UTCZoneId);
            }
        }
    }
}
