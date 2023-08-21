package ru.practicum.user;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserListener implements MessageListener<UserDto> {

    private static final String EVENT_NAME_MESSAGE_SUCCESSFULLY_SENT = "{} message successfully sent!";
    private static final String KEY = "Key: {}";
    private static final String MESSAGE_ID = "MessageId: {}";
    private static final String EVENT_NAME_COLON_MESSAGE = "{} : {}";
    private static final String MESSAGE_RECEIVED = "Message received. MessageId: {}, Key: {}, Value: {}, Properties: {}";

    @Override
    public void received(Consumer<UserDto> consumer, Message<UserDto> msg) {
        try {
            // Do something with the message
            log.info(MESSAGE_RECEIVED,
                    msg.getMessageId(), msg.getKey(), new String(msg.getData()), msg.getProperties());

            // Acknowledge the message
            consumer.acknowledge(msg);
        } catch (Exception e) {
            // Message failed to process, redeliver later
            consumer.negativeAcknowledge(msg);
        }
    }

    @Override
    public void reachedEndOfTopic(Consumer<UserDto> consumer) {
        MessageListener.super.reachedEndOfTopic(consumer);
    }
}
