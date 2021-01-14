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

        /*
        Assign the consumer to a consumer group. Without being assigned to a group, the consumer won't be able to read from Kafka.
        Because the messages are only read once per consumer group, the messages won't be read again if you restart the application.
        If you want to achieve this, you have to change the consumer group or to disable auto-committing (see next properties).
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Exercise");
        /*
        "Earliest" is not the same as "--from-beginning" as when using the console-consumer! It only means that if there is no initial offset,
        it is automatically set to the earliest. Using this property, the application will read from the beginning but only when starting
        it for the first time.
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /*
        By disabling auto-committing, the messages will be read again every time.
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        /*
        The application should always be properly quit, especially when using a while-loop like in this example.
        By using a ShutDownHook, the following logic is executed when the application is quit and the consumer is properly closed.
        'running' will be set to false, therefore the while-loop will be quit and the finally-block will be executed.
         */
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
            consumer.subscribe(Collections.singleton("numbers"));
            //using a while loop to constantly poll for messages
            while (running.get()) {
                /*
                The argument of the poll() method sets a limit for the waiting time for new messages. I.e. when a message arrives
                within this time interval, the consumer is NOT blocked. The message is directly forwarded and further logic can continue.
                In this example, the consumer waits at most (!) 2000ms before returning to continue executing the program.
                 */
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
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
