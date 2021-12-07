package se.rise.kafkatest.producer;
import io.hops.util.Hops;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKafkaHops {

    private static final Logger log = LoggerFactory.getLogger(ProducerKafkaHops.class);

    private SparkSession sparkSession;
    private KafkaProducer<String, String> myProducer;

    public Callback callback = new Callback() {
        public void onCompletion(RecordMetadata metadata, java.lang.Exception exception) {
            if (exception != null) {
                log.warn("Callback received - exception", exception);
            } else if (metadata != null) {
                log.debug("Callback received - ACK: {}", metadata);
            }
        }
    };

    public KafkaProducer<String, String> getKafkaProducer() {
        return myProducer;
    }

    public ProducerKafkaHops() {
        Properties properties = Hops.getKafkaSSLProperties();
        sparkSession = Hops.findSpark();

        log.info("Starting Streaming Kafka data with Spark Job (Hopsworks): {}", sparkSession.logName());

        // hopefully this is the correct way to do it
        String eps = Hops.getBrokerEndpoints();

        // set the bootstrap server
        properties.put("bootstrap.servers", eps);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        myProducer = new KafkaProducer<>(properties);
    }

    public void shutdown() {
        myProducer.close();
        sparkSession.stop();
    }

    public static void main(String[] args) {
        if (args.length < 1) {  
            System.out.println("Usage: java ProducerKafkaHops <topic>");
            System.exit(1);     
        }
        ProducerKafkaHops hopsProducer = new ProducerKafkaHops();
        KafkaProducer<String, String> producer = hopsProducer.getKafkaProducer();

        String topic = args[0];
        System.out.println("Producing for topic: " + topic);

        try {
            for (int i = 0; i < 10; i++) {
                String v = "{\"quantityKind\":\"temperature\",\"value\":" + Integer.toString(i + 1) + "}";
                producer.send(new ProducerRecord<>(topic, "value", v), hopsProducer.callback);
                producer.flush();
            }
        } catch (Exception e) {
            System.out.println("Kafka Failed...");
            e.printStackTrace();
        } finally {
            hopsProducer.shutdown();
        }
    }
}
