import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

        String bootstrapServer = "127.0.0.1:29092";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // none, latest

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign and seek is usually used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offSetToReadFrom = 15L;
        consumer.assign(List.of(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offSetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while (keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord record : records){
                numberOfMessagesReadSoFar++;
                logger.info("[KAFKA] - key {}, value {}", record.key(), record.value());
                logger.info("[KAFKA] - partition {}, offset {}", record.partition(), record.offset());
                logger.info("[KAFKA] - topic {}", record.topic());
                logger.info("[KAFKA] - headers {}", record.headers().toString());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
