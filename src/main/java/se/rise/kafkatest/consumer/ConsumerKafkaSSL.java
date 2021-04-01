package se.rise.kafkatest.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import se.rise.kafkatest.KafkaConfig;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaSSL {

    public static void main(String[] args) {
        Properties properties = new Properties();
        if (args.length < 1) {
            System.out.println("Please provide password for keystore");
            return;
        }
        String password = args[0];
        properties.put("bootstrap.servers", KafkaConfig.HOST);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "something");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", "trustStore.jks");
        properties.put("ssl.truststore.password", password);
        properties.put("ssl.keystore.location", "keyStore.jks");
        properties.put("ssl.keystore.password", password);
        properties.put("ssl.key.password", password);

        final Callback callback = new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.out.println("Callback received - exception:" + exception.getMessage());
                } else if (metadata != null) {
                    System.out.println("Callback received - ACK:" + metadata.toString());
                }
            }
        };
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(KafkaConfig.TOPIC));

        try {
            /* Try to consume 100 messages - or 100 timeouts */
            for (int i = 0; i < 100; i++) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                System.out.println("Received: " + records.count() + " records.");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Simple String message= " + String.valueOf(record.value()));
                }
            }
        } catch (Exception e) {
            System.out.println("Kafka Failed...");
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
