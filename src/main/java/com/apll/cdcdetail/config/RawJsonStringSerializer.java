package com.apll.cdcdetail.config;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
@Component
public class RawJsonStringSerializer implements Serializer<String> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration needed
    }

    @Override
    public byte[] serialize(String topic, String data) {
        if (data == null)
            return null;
        return data.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        // No resources to be released
    }
}
