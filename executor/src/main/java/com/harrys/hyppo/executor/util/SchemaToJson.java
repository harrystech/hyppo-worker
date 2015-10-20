package com.harrys.hyppo.executor.util;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

/**
 * Created by jpetty on 7/22/15.
 */
public final class SchemaToJson {

    public static final class Serializer extends JsonSerializer<Schema> {

        public Serializer(){ }

        @Override
        public final void serialize(Schema value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
            jgen.writeRawValue(value.toString(false));
        }
    }

    public static final class Deserializer extends JsonDeserializer<Schema> {

        public Deserializer(){ }

        @Override
        public final Schema deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            final ObjectNode node = (ObjectNode)jp.getCodec().readTree(jp);
            return new Schema.Parser().parse(node.toString());
        }
    }
}
