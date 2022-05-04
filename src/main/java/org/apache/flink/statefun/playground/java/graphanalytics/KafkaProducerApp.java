package org.apache.flink.statefun.playground.java.graphanalytics;

import java.io.File;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * KafkaProducerApp sends line by line of a textfile to Kafka
 */
public class KafkaProducerApp {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting KafkaProducerApp...");

        String textFile = args[0].trim();
        if(textFile.isEmpty()) {
            throw new Exception("Name of the text file is required as input!");
        }else if(!args[0].endsWith(".txt")) {
            textFile += ".txt";
        }
        System.out.println(String.format("Reading %s file", textFile));
        File fileName = new File("./data/" + textFile);
        Scanner scFiles = new Scanner(fileName);

        // environment variables
        String kafkaAddress = System.getenv("BROKER_ADDRESS") == null ? "localhost" : System.getenv("BROKER_ADDRESS");
        String kafkaPort = System.getenv("BROKER_ADDRESS_PORT") == null ? "9092" : System.getenv("BROKER_ADDRESS_PORT");
        System.out.println(String.format("%s:%s", kafkaAddress, kafkaPort));
        // Kafka Producer API
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%s", kafkaAddress, kafkaPort));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        final String topicName = "tasks";
        while(scFiles.hasNextLine()) {
            // create message
            String[] inputStr = scFiles.nextLine().trim().split(" ");
            String jsonString = String.format("{\"task\": \"ADD\", \"src\": \"%1$s\", \"dst\": \"%2$s\", \"t\": \"%3$s\", \"endTime\": \"0\"}", inputStr[0], inputStr[1], inputStr[2]);
            System.out.println(jsonString);

            try {
                producer.send(new ProducerRecord<>(topicName, inputStr[0], jsonString)).get();
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Finished sending!");
        producer.close();
        scFiles.close();
    }
}
