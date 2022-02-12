import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello from Java!! message " + i);

            // send data
            producer.send(record, (recordMetadata, e) -> {
                if (e == null){
                    // record was successfully sent
                    logger.info("received new metadata. \n" +
                            "topic: " + recordMetadata.topic() + "\n" +
                            "partition: " + recordMetadata.partition() + "\n" +
                            "offset: " + recordMetadata.offset() + "\n" +
                            "timestamp: " + recordMetadata.timestamp());

                } else {
                    logger.error(e.getMessage());
                }
            }) ;
        }

        // flush message
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
