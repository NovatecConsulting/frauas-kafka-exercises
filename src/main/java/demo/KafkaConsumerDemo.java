package demo;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class KafkaConsumerDemo {
    static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //TODO: add the ConsumerConfig properties for key and value deserializer (String)
        //TODO: add another important ConsumerConfig property to make this example work!
        //TODO: add two ConsumerConfig properties to make sure that the consumer always reads from the beginning. Which properties are needed?

        //TODO: Additional task (advanced), part 1: make sure that the program can be quit properly using a ShutDownHook and a CountDownLatch

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton("words"));
            //TODO: the application should constantly poll for new messages. How could you enable this?

            //TODO: what happens if you increase the argument of the poll() method to e.g. 5000ms? And what happens if you send a message?
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000)); //wait for messages
            if (records.count() != 0) { //only print when new messages were read
                System.out.println("Records read:" + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    //TODO: print the messages to the console (Offset, Key and Value)
                }
            }
        }finally {
            LOGGER.info("Stopped.");
            //TODO: Additional task (advanced), part 2: make sure that the program can be quit properly using a ShutDownHook and a CountDownLatch
        }
    }
}
