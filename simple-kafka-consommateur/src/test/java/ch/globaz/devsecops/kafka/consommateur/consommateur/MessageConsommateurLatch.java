package ch.globaz.devsecops.kafka.consommateur.consommateur;

import ch.globaz.devsecops.kafka.common.HelloWorld;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

import java.util.concurrent.CountDownLatch;

/**
 * Created by seb on .
 * <p>
 * ${VERSION}
 */
@Slf4j
public class MessageConsommateurLatch extends MessageConsommateur {

    private CountDownLatch latch = new CountDownLatch(5);

    public CountDownLatch getLatch(){
        return latch;
    }

    private final String GROUP_NAME  = "test-kafka-group";

    @KafkaListener(groupId = "test-kafka-group",containerFactory = "kafkaListenerContainerFactory",
            topicPartitions = {@TopicPartition(topic = "${test.topic.name}",
                    //consommation des messages depuis le denut, a des fins de tests
                    partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}
            )})
    @Override
    public void stringListener(HelloWorld message) {
        log.info("Message reçu: {}",message);
        latch.countDown();
    }
}
