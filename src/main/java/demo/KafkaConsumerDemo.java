package demo;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerDemo {
    static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Exercise"); //Assign consumer to a consumer group
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                countDownLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("No grateful termination possible.");
            }
        }
        ));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton("words"));
            //wait for messages
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000)); //wait at most 2000ms
                if (records.count() != 0) { //only print when new messages were read
                    System.out.println("Records read:" + records.count());
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("offset:" + record.offset() + ", key: " + record.key() + ", value: " + record.value());
                    }
                }
            }
        }finally {
            LOGGER.info("Stopped.");
            countDownLatch.countDown();
        }
    }
}
