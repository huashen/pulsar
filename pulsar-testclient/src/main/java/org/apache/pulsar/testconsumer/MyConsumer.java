package org.apache.pulsar.testconsumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * MyConsumer
 *
 * @author longhuashen
 * @since 2020-11-29
 */
public class MyConsumer {

    private static final Logger log = LoggerFactory.getLogger(MyConsumer.class);

    private static final String SERVER_URL = "pulsar://localhost:6650";

    public static void main(String[] args) throws Exception {
        // 构造Pulsar Client
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVER_URL)
                .enableTcpNoDelay(true)
                .build();

        Consumer consumer = client.newConsumer()
                .consumerName("my-consumer")
                .topic("my-topic")
                .subscriptionName("my-subscription")
                .ackTimeout(10, TimeUnit.SECONDS)
                .maxTotalReceiverQueueSizeAcrossPartitions(10)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        while (true) {
            // Wait for a message
            Message msg = consumer.receive();
            try {
                // Do something with the message
                System.out.printf("Message received: %s\n", new String(msg.getData()));
                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }
    }
}
