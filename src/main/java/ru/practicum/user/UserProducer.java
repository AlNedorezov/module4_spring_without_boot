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

@Slf4j
@Service
@RequiredArgsConstructor
public class UserProducer {

    @Autowired
    private final PulsarClient pulsarClient;

    private Producer<UserDto> producer;
    private static final String AGGREGATE_NAME = "user";
    private static final String UNKNOWN = "unknown";
    private static final String EVENT_NAME_MESSAGE_SUCCESSFULLY_SENT = "{} message successfully sent!";
    private static final String KEY = "Key: {}";
    private static final String MESSAGE_ID = "MessageId: {}";
    private static final String EVENT_NAME_COLON_MESSAGE = "{} : {}";
    private static final String EVENT_FAIL = "Failed to sent {} message";

    @PostConstruct
    private void postConstruct() throws PulsarClientException {
        producer = pulsarClient.newProducer(JSONSchema.of(UserDto.class))
                .topic("add-user")
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
                    .send();
            final String eventName = UserDto.class.getSimpleName();
            log.info(EVENT_NAME_MESSAGE_SUCCESSFULLY_SENT, eventName);
            log.info(KEY, keyString);
            log.info(EVENT_NAME_COLON_MESSAGE, eventName, message.toString());
            log.info(MESSAGE_ID, messageId);
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
