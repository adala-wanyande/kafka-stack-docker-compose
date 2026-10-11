package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("hello world!");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("batch.size", "400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Send 30 batches of 10 messages (300 messages total)
        try {
            for (int batch = 1; batch <= 10; batch++) {
                for (int i = 1; i <= 30; i++) {

                    String message = "Batch " + batch + " - Message " + i;

                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>("demo_java", message);

                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(
                                RecordMetadata recordMetadata, Exception e) {

                            if (e == null) {
                                log.info("Message delivered successfully:\n" +
                                        "Topic: " + recordMetadata.topic() + "\n" +
                                        "Partition: " + recordMetadata.partition() + "\n" +
                                        "Offset: " + recordMetadata.offset() + "\n" +
                                        "Timestamp: " + recordMetadata.timestamp());
                            } else {
                                log.error("Error while producing message", e);
                            }
                        }
                    });

                    // Sleep for 500 milliseconds between messages
                    Thread.sleep(500);
                }

                log.info("Completed batch " + batch + " of 30");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Message production interrupted", e);
        } finally {
            // Ensure outstanding messages are sent before closing
            producer.flush();
            producer.close();
        }

        log.info("Finished producing messages.");
    }
}