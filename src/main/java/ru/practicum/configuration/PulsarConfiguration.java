package ru.practicum.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.text.SimpleDateFormat;

@Configuration
@RequiredArgsConstructor
public class PulsarConfiguration {
    public static final String DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX";

    @Bean
    public PulsarClient getClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl("pulsar://192.168.3.18:6650")
                .build();
    }

    @Bean
    public ObjectMapper pulsarObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat(DATETIME_PATTERN));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(MapperFeature.ALLOW_COERCION_OF_SCALARS);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        return objectMapper;
    }
}
