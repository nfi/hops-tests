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
        /* Default loop count unless specified otherwise using argument */
        int count = 100;
        for (int i = 0; i < args.length; i += 2) {
            if (i >= args.length - 1) {
                usage("Too few arguments");
                return;
            }
            String name = args[i];
            String value = args[i + 1];
            if ("-c".equals(name)) {
                count = Integer.parseInt(value);
            } else if ("-t".equals(name)) {
                topic = value;
            } else if ("-k".equals(name)) {
                sharedAccessKey = value;
            } else if ("-h".equals(name)) {
                usage("");
                return;
            } else {
                usage("Unknown argument \"" + name + "\"!");
                return;
            }
        }
        try {
            if (sharedAccessKey == null) {
                sharedAccessKey = Hops.getSecret("EventHubKey");
            }
            if (topic == null) {
                topic = Hops.getSecret("KafkaTopic");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        /* If not secrets are available - read arguments */
        if (sharedAccessKey == null) {
            usage("No access key found!");
            return;
        }
        if (topic == null) {
            usage("No Kafka topic found!");
            return;
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
            /* Try to consume specified number of messages - or timeouts */
            for (int i = 0; i < count || count == 0; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                System.out.println("--------------------");
                System.out.println("Received: " + records.count() + " records.");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Simple String message received: " + record.value());
                    producer.send(new ProducerRecord<>(topic, record.key(), record.value()), hopsProducer.callback);
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

    private static void usage(String message) {
        if (!message.isEmpty()) {
            System.err.println("Error: " + message);
        }
        System.out.println("Usage: " + Eventhub2Hops.class.getName() + " [-c count] [-t topic] [-k access-key]");
        System.exit(message.isEmpty() ? 0 : 2);
    }

}
