package se.rise.kafkatest.producer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import se.rise.kafkatest.KafkaConfig;
import io.hops.util.Hops;

public class ProducerKafkaHops {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties = Hops.getKafkaSSLProperties();

        if (args.length < 1) {  
            System.out.println("Usage: java ProducerKafkaHops <topic>");
            System.exit(1);     
        }
        String topic = args[0];

        properties.put("bootstrap.servers", KafkaConfig.HOST);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("ssl.endpoint.identification.algorithm", "");

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
                myProducer.send(new ProducerRecord<String, String>(topic,
                                "value", v), callback);
            }
        } catch (Exception e) {
            System.out.println("Kafka Failed...");
            e.printStackTrace();
        } finally {
            myProducer.close();
        }
    }
}
