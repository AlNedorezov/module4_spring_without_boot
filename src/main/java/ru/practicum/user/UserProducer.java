package ru.practicum.user;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.practicum.pulsar.AggregateMessageKey;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserProducer {

    @Autowired
    private final PulsarClient pulsarClient;

    private Producer<UserDto> producer;
    private static final String AGGREGATE_NAME = "user";
    private static final String UNKNOWN = "unknown";
    private static final String TOPIC_NAME = "add-user";
    private static final String EVENT_NAME_MESSAGE_SUCCESSFULLY_SENT =
            "{} message successfully sent! MessageId: {}, Key: {}, Value: {}, Properties: {}";
    private static final String EVENT_FAIL = "Failed to sent {} message";

    Map<String, String> massageProperties = Map.of("data_source", "rest-api");

    @PostConstruct
    private void postConstruct() throws PulsarClientException {
        producer = pulsarClient.newProducer(JSONSchema.of(UserDto.class))
                .topic(TOPIC_NAME)
                .create();
    }

    @PreDestroy
    private void onPreDestroy() throws PulsarClientException {
        producer.close();
    }

    private void send(final AggregateMessageKey key, final UserDto message) {
        final String keyString = key.toJsonString();
        try {
            MessageId messageId = producer
                    .newMessage()
                    .key(keyString)
                    .value(message)
                    .properties(massageProperties)
                    .send();
            final String eventName = UserDto.class.getSimpleName();
            log.info(EVENT_NAME_MESSAGE_SUCCESSFULLY_SENT,
                    eventName, messageId, keyString, message.toString(), massageProperties);
        } catch (PulsarClientException e) {
            log.error(EVENT_FAIL, UserDto.class.getSimpleName());
        }
    }

    public void send(final UserDto user) {
        AggregateMessageKey aggregateMessageKey = new AggregateMessageKey(
                AGGREGATE_NAME,
                user.getId() != null ? user.getId().toString() : UNKNOWN);
        send(aggregateMessageKey, user);
    }
}
