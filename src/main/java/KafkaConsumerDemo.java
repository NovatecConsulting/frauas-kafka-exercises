import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.*;

public class KafkaConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "1"); //Assign consumer to a consumer group

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("numbers"));

        try {
            //wait for messages
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.count() != 0) { //only print when new messages were read
                    System.out.println("Records read:" + records.count());
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("offset:" + record.offset() + ", key: " + record.key() + ", value: " +record.value());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
