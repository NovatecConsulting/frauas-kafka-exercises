package demo;

import org.apache.kafka.clients.producer.*;

import java.util.*;

public class KafkaProducerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //key is optional, but should be provided to assign the message to a specific partition
        ProducerRecord<String, String> record = new ProducerRecord<>("words", "TestKey", "Hello World!");

        try(Producer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(record);
        }
    }
} 
