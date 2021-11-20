package se.rise.kafkatest.eventhub2hops;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.hops.util.Hops;
import se.rise.kafkatest.producer.ProducerKafkaHops;
import se.rise.kafkatest.consumer.ConsumerKafkaSASL_SSL;
import java.time.Duration;

public class Eventhub2Hops {
    public static void main(String[] args) {
        String sharedAccessKey = null;
        String topic = null;
        try {
            sharedAccessKey = Hops.getSecret("EventHubKey");
            topic = Hops.getSecret("KafkaTopic");
            System.out.println("Topic:" + topic);
        } catch (Exception e) {
            e.printStackTrace();
        }
        /* If not secrets are available - read arguments */
        if (sharedAccessKey == null && args.length < 2) {  
            System.out.println("Usage: java Eventhub2Hops <EventHub Shared access key> <hopstopic>");
            System.exit(1);
        }
        if (sharedAccessKey == null) {
            sharedAccessKey = args[0];
        }
        if (topic == null) {
            topic = args[1];
        }
        /* Setup Hops producer */
        ProducerKafkaHops hopsProducer = new ProducerKafkaHops();
        KafkaProducer<String, String> producer = hopsProducer.getKafkaProducer();

        /* Setup Hops consumer */        
        ConsumerKafkaSASL_SSL kafkaClient = new ConsumerKafkaSASL_SSL(sharedAccessKey);
        /* use default properties */
        kafkaClient.connect(null);
        KafkaConsumer<String, String> consumer = kafkaClient.getKafkaConsumer();

        System.out.println("Producing to Hops for topic: " + topic);

        try {
            /* Try to consume 100 messages - or 100 timeouts */
            for (int i = 0; i < 100; i++) {
                ConsumerRecords<String, String> records =consumer.poll(Duration.ofSeconds(5));
                System.out.println("--------------------");
                System.out.println("Received: " + records.count() + " records.");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Simple String message received: " + String.valueOf(record.value()));
                    producer.send(new ProducerRecord<String, String>(topic,
                                record.key(), record.value()), hopsProducer.callback);
                    producer.flush();
                }
                System.out.println("--------------------");
            }
        } catch (Exception e) {
            System.out.println("Kafka Failed...");
            e.printStackTrace();
        } finally {
            kafkaClient.close();
            hopsProducer.shutdown();
        }
    }
}

