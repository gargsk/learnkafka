package org.sharad;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer{
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class);
        String grpId = "app5";
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grpId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList("first-topic"));
        while(true){
            ConsumerRecords<String,String> recs = consumer.poll(Duration.ofMillis(100));
            recs.forEach(t -> {
                logger.info("Key: "+t.key()+ " Value: "+ t.value());
                logger.info("Partition: "+t.partition()+ " Offset: "+ t.offset());
            });
        }



    }
}