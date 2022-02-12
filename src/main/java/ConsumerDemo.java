import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServer = "127.0.0.1:29092";
        String groupId = "my-fourth-app";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // none, latest

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records){
                logger.info("[KAFKA] - key {}, value {}", record.key(), record.value());
                logger.info("[KAFKA] - partition {}, offset {}", record.partition(), record.offset());
                logger.info("[KAFKA] - topic {}", record.topic());
                logger.info("[KAFKA] - headers {}", record.headers().toString());
            }
        }
    }
}
