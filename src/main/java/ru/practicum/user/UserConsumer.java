package ru.practicum.user;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserConsumer {
    @Autowired
    private final PulsarClient pulsarClient;

    private Consumer<UserDto> consumer;

    private final UserListener userListener;
    private static final String TOPIC_NAME = "add-user";
    private static final String SUBSCRIPTION_NAME = "module4-app-subscription";

    @PostConstruct
    private void postConstruct() throws PulsarClientException {
        consumer = pulsarClient.newConsumer(JSONSchema.of(UserDto.class))
                .topic(TOPIC_NAME)
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Key_Shared)
                .messageListener(userListener)
                .subscribe();
    }

    @PreDestroy
    public void preDestroy() throws PulsarClientException {
        consumer.close();
    }
}
