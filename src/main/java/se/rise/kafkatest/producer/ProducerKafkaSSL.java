package se.rise.kafkatest.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

public class ProducerKafkaSSL {
    public static void main(String[] args) {
        Properties properties = new Properties();
        String password = "RR8YCPXAKWE2NDWG9U59O0TY2I1KQB6XHA29UJSIS643FH7NSBGJ3GIY6WRGYW8F";
        properties.put("bootstrap.servers", "hops.site:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", "trustStore.jks");
        properties.put("ssl.truststore.password", password);
        properties.put("ssl.keystore.location", "keyStore.jks");
        properties.put("ssl.keystore.password", password);
        properties.put("ssl.key.password", password);

        final Callback callback = new Callback() {
            public void onCompletion(RecordMetadata metadata, java.lang.Exception exception) {
                if (exception != null) {
                    System.out.println("Callback received - exception:" + exception.getMessage());
                } else if (metadata != null) {
                    System.out.println("Callback received - ACK:" + metadata.toString());
                }
            }
        };

        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(properties);

        try {
            for (int i = 0; i < 10; i++) {
                String v = "{\"quantityKind\":\"temperature\",\"value\":" + Integer.toString(i + 1) + "}";
                myProducer.send(new ProducerRecord<String, String>("DemoKafkaTopic_9907",
                                "value", v),
                        callback);
            }
        } catch (Exception e) {
            System.out.println("Kafka Failed...");
            e.printStackTrace();
        } finally {
            myProducer.close();
        }
    }
}