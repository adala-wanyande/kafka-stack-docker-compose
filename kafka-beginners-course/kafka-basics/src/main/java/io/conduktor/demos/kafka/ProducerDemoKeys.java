package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Starting Kafka producer with the same key!");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(properties)) {

            String key = "customer_123";

            // Send 10 messages with the same key
            for (int i = 1; i <= 10; i++) {

                String value = "Message number " + i;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(
                            RecordMetadata recordMetadata, Exception e) {

                        if (e == null) {
                            log.info(
                                    "Message delivered successfully | " +
                                            "Key: {} | Value: {} | Partition: {} | Offset: {}",
                                    key,
                                    value,
                                    recordMetadata.partition(),
                                    recordMetadata.offset()
                            );
                        } else {
                            log.error("Error while producing message", e);
                        }
                    }
                });

                // Wait 500 milliseconds between messages
                if (i < 10) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Producer interrupted", e);
                        break;
                    }
                }
            }

            // Ensure all messages are sent before closing
            producer.flush();
        }

        log.info("Finished producing messages.");
    }
}