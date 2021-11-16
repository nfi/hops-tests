package se.rise.kafkatest.producer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import io.hops.util.Hops;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class ProducerKafkaHops {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties = Hops.getKafkaSSLProperties();
        SparkSession s = Hops.findSpark();
        Logger log = LogManager.getLogger(ProducerKafkaHops.class);
        log.setLevel(Level.INFO);

        log.info("Starting Streaming Kafka data with Spark Job (Hopsworks):" + s.logName());

        if (args.length < 1) {  
            System.out.println("Usage: java ProducerKafkaHops <topic>");
            System.exit(1);     
        }
        String topic = args[0];
        System.out.println("Producing for topic: " + topic);

        // hopefully this is the correct way to do it
        String eps = Hops.getBrokerEndpoints();

        // set the bootstrap server
        properties.put("bootstrap.servers", eps);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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
                log.info("Kafka message:" + i);
                String v = "{\"quantityKind\":\"temperature\",\"value\":" + Integer.toString(i + 1) + "}";
                myProducer.send(new ProducerRecord<String, String>(topic,
                                "value", v), callback);
                myProducer.flush();
            }
        } catch (Exception e) {
            System.out.println("Kafka Failed...");
            e.printStackTrace();
        } finally {
            myProducer.close();
        }
    }
}
