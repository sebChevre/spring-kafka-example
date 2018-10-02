package ch.globaz.devsecops.kafka.producteur.producteur;

import ch.globaz.devsecops.kafka.common.HelloWorld;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;


public class MessageProducteur {

    @Autowired
    private KafkaTemplate<String, HelloWorld> kafkaTemplate;


    @Value(value = "${test.retry.topic.name}")
    private String topicName;


    public void sendMessage(HelloWorld message) {

        kafkaTemplate.send(topicName, message);
    }

}
