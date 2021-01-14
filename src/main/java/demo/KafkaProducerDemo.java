package demo;

import org.apache.kafka.clients.producer.*;

import java.util.*;

public class KafkaProducerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //TODO: add the two ProducerConfig properties for the key and value serializer (String)

        //TODO: Create a new record using the class ProducerRecord
        //note: key is optional, but should be provided to assign the message to a specific partition

        //TODO: Create a new producer with the above specified properties using the class Producer
        try(Producer<String, String> producer = new KafkaProducer<>(properties)) {
            //TODO: send the record to Kafka
        }
    }
} 
