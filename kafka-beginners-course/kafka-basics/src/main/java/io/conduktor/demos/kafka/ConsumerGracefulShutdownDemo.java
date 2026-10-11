
package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGracefulShutdownDemo {

    private static final Logger log =
            LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Starting Kafka consumer");

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
        properties.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG,
                "my-java-application"
        );
        properties.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(properties);

        // Gracefully interrupt the poll loop when the JVM shuts down.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received. Waking up consumer...");
            consumer.wakeup();
        }));

        try (consumer) {

            consumer.subscribe(Collections.singleton("demo_java"));

            while (true) {
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

        } catch (WakeupException e) {
            // Expected when consumer.wakeup() is called during shutdown.
            log.info("Consumer is shutting down gracefully.");
        } finally {
            log.info("Consumer closed.");
        }
    }
}
