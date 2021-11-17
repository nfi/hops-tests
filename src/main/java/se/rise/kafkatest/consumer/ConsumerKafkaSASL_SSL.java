package se.rise.kafkatest.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/* Example that consumes Kafka messages from IDUN / Proptech OS 
*  For Fastighetsdatalabbet.
*
*
*/

public class ConsumerKafkaSASL_SSL {

    private static String HOST = "idun-multiprod-streamingapi-fastighetsdatalabbet.servicebus.windows.net";
    private static String KAFKA_HOST = HOST + ":9093";
    private static String TOPIC = "idun-multiprod-eventhub-recipient-fastighetsdatalabbet";

    private KafkaConsumer<String, String> consumer;
    private String sharedAccessKey;

    public ConsumerKafkaSASL_SSL(String sharedAccessKey) {
        this.sharedAccessKey = sharedAccessKey;
    }

    public void connect(Properties properties) {
        if (properties == null) {
            properties = new Properties();
        }

        if (properties.getProperty("bootstrap.servers") == null) {
            properties.put("bootstrap.servers", KAFKA_HOST);
        }

        properties.put("bootstrap.servers", KAFKA_HOST);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "$Default");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://" + HOST + "/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=" + sharedAccessKey + "\";");

        final Callback callback = new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.out.println("Callback received - exception:" + exception.getMessage());
                } else if (metadata != null) {
                    System.out.println("Callback received - ACK:" + metadata.toString());
                }
            }
        };

        /* This is a test for IoT Hub / Fastighetsdatalabbet - lets just consume from EventHub name (TOPIC)*/
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(TOPIC));
    }

    public void close() {
        consumer.close();
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return consumer;
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        if (args.length < 1) {
            System.out.println("Please provide Shared Access key for:" + HOST);
            return;
        }

        String sharedAccessKey = args[0];

        ConsumerKafkaSASL_SSL kafkaClient = new ConsumerKafkaSASL_SSL(sharedAccessKey);
        /* use default properties */
        kafkaClient.connect(null);

        try {
            /* Try to consume 100 messages - or 100 timeouts */
            for (int i = 0; i < 100; i++) {
                ConsumerRecords<String, String> records = kafkaClient.consumer.poll(Duration.ofSeconds(5));
                System.out.println("--------------------");
                System.out.println("Received: " + records.count() + " records.");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Simple String message= " + String.valueOf(record.value()));
                }
                System.out.println("--------------------");
            }
        } catch (Exception e) {
            System.out.println("Kafka Failed...");
            e.printStackTrace();
        } finally {
          
        }
    }
}

