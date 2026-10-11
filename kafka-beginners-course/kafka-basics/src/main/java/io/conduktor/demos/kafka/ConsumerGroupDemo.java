
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

public class ConsumerGroupDemo {

    private static final Logger log =
            LoggerFactory.getLogger(ConsumerGroupDemo.class.getSimpleName());

    public static void main(String[] args) {

        String consumerName = args.length > 0 ? args[0] : "consumer-1";

        log.info("{} starting...", consumerName);

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

        // All instances must use the same group ID.
        properties.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG,
                "java-consumer-group"
        );

        properties.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("{} received shutdown signal", consumerName);
            consumer.wakeup();
        }));

        try (consumer) {

            consumer.subscribe(Collections.singleton("demo_java"));

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info(
                            "{} | Key: {} | Value: {} | Partition: {} | Offset: {}",
                            consumerName,
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset()
                    );
                }
            }

        } catch (WakeupException e) {
            log.info("{} shutting down gracefully", consumerName);
        } finally {
            log.info("{} closed", consumerName);
        }
    }
}
