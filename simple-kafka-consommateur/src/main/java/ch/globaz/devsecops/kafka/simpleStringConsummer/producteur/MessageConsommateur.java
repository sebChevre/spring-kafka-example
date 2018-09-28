package ch.globaz.devsecops.kafka.simpleStringConsummer.producteur;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

/**
 * Classe consommatrices des messages kafka
 */
public class MessageConsommateur {

    @KafkaListener(groupId = "test-kafka-group",containerFactory = "kafkaListenerContainerFactory",
            topicPartitions = {@TopicPartition(topic = "${test.topic.name}",
                    //consommation des messages depuis le denut, a des fins de tests
                    partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}
            )})
    public void stringListener(String message) {
        System.out.println("Message reçu: " + message);

    }

}
