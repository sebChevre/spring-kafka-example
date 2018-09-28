package ch.globaz.devsecops.kafka.simpleStringProducer.producteur;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;


public class MessageProducteur {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @Value(value = "${test.topic.name}")
    private String topicName;


    public void sendMessage(String message) {
        kafkaTemplate.send(topicName, message);
    }

}
