package org.sharad;

import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallBack{
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,String> rec = new ProducerRecord<>("first-topic", RandomStringUtils.randomAlphabetic(10));
            producer.send(rec, new Callback(){
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception ==null){
                        logger.info("Topic: "+ metadata.topic());
                        logger.info("Partition: "+ metadata.partition());
                        logger.info("Topic: "+ metadata.offset());
                        logger.info("Topic: "+ metadata.timestamp());
                    }else{

                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}