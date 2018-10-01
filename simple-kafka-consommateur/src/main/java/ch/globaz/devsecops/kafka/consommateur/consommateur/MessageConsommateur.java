package ch.globaz.devsecops.kafka.consommateur.consommateur;


import ch.globaz.devsecops.kafka.common.HelloWorld;
import ch.globaz.devsecops.kafka.consommateur.producteur.MessageProducteur;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jms.JmsProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


/**
 * Classe consommatrices des messages kafka
 */
@Component
public class MessageConsommateur {

    @Autowired
    MessageProducteur messageProducteur;

    @KafkaListener(id="listener",topics = "${test.topic.name}",groupId = "test-kafka-group2",
            containerFactory = "kafkaListenerContainerFactory",errorHandler = "errorHandler"
    ,topicPartitions =  {@TopicPartition(topic = "${test.topic.name}", partitions = { "0"})})
    public void messageListener(ConsumerRecord<String,HelloWorld> cr, Acknowledgment acknowledgment) {

        System.out.println("offset: " + cr.offset());
        System.out.println("Message reçu [valeur]: " + cr.value());
        System.out.println("Message reçu [cle]: " + cr.key());

        if(cr.value().getNumero() %2 == 0){
            throw new NullPointerException();
        }else{
            acknowledgment.acknowledge();
        }

    }

}
