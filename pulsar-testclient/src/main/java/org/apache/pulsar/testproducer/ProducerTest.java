package org.apache.pulsar.testproducer;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * ProducerTest
 *
 * @author longhuashen
 * @since 2020-11-29
 */
public class ProducerTest {

    private static final Logger log = LoggerFactory.getLogger(ProducerTest.class);

    private static final String SERVER_URL = "pulsar://localhost:6650";

    public static void main(String[] args) throws Exception {
        // 构造Pulsar Client
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVER_URL)
                .enableTcpNoDelay(true)
                .build();
        // 构造生产者
        Producer<String> producer = client.newProducer(Schema.STRING)
                .producerName("my-producer")
                .topic("my-topic")
                .batchingMaxMessages(1024)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .enableBatching(true)
                .blockIfQueueFull(true)
                .maxPendingMessages(512)
                .sendTimeout(10, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();
        // 同步发送消息
        MessageId messageId = producer.send("Hello World to longmao");
        log.info("message id is {}", messageId);
        System.out.println(messageId.toString());

        producer.close();
        client.close();


//        // 异步发送消息
//        CompletableFuture<MessageId> asyncMessageId = producer.sendAsync("This is a async message");
//        // 阻塞线程，直到返回结果
//        log.info("async message id is {}", asyncMessageId.get());
//        client.closeAsync();
    }
}
