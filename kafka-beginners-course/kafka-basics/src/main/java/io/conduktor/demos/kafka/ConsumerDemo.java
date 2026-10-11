package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log =
            LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Starting Kafka consumer!");

        // Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092"
        );
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        // Consumer Group
        properties.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG,
                "my-java-application"
        );

        // Start reading from the earliest available offset
        properties.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        // Create the Consumer
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(properties)) {

            // Subscribe to the topic
            consumer.subscribe(Collections.singleton("demo_java"));

            // Poll for messages continuously
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info(
                            "Key: {} | Value: {} | Partition: {} | Offset: {}",
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset()
                    );
                }
            }
        }
    }
}