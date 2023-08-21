package ru.practicum.user;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserListener implements MessageListener<UserDto> {

    private final UserService userService;
    private static final String MESSAGE_RECEIVED =
            "{} message received. MessageId: {}, Key: {}, Value: {}, Properties: {}";

    private static final String USER_SUCCESSFULLY_SAVED =
            "User with id={} successfully saved!";

    private static final String ENCOUNTERED_PROBLEMS_WHILE_SAVING_USER =
            "Problems were encountered while saving the user!";

    @Override
    public void received(Consumer<UserDto> consumer, Message<UserDto> msg) {
        try {
            final String eventName = UserDto.class.getSimpleName();
            log.info(MESSAGE_RECEIVED,
                    eventName, msg.getMessageId(), msg.getKey(), new String(msg.getData()), msg.getProperties());

            final UserDto savedUser = userService.saveUser(msg.getValue());

            log.info(USER_SUCCESSFULLY_SAVED, savedUser.getId());

            // Acknowledge the message
            consumer.acknowledge(msg);
        } catch (PulsarClientException | NullPointerException e) {
            log.info(ENCOUNTERED_PROBLEMS_WHILE_SAVING_USER);
            // Message failed to process, redeliver later
            consumer.negativeAcknowledge(msg);
        }
    }

    @Override
    public void reachedEndOfTopic(Consumer<UserDto> consumer) {
        MessageListener.super.reachedEndOfTopic(consumer);
    }
}
