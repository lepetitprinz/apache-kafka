package com.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("Kafka Producer");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<2; i++) {
            for (int j=0; j<10; j++) {
                String topic = "demo-java";
                String key = "id-" + j;
                String value = "hello world " + j;

                // create a Producer Record
                ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);
                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or exception is thrown
                        if (e == null) {
                            // the record was successfully set
                            log.info("key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // flush ald close the producer
        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
