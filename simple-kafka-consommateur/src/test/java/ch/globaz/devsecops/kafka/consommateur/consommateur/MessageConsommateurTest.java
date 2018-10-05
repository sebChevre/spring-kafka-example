package ch.globaz.devsecops.kafka.consommateur.consommateur;

import ch.globaz.devsecops.kafka.common.HelloWorld;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@EnableKafka
@Slf4j
@ExtendWith(SpringExtension.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1,controlledShutdown = true,topics = "${kafka.topic.test.name}" )
@SpringBootTest(classes = {MessageConsommateurTestConfig.class})
@ActiveProfiles("test")
class MessageConsommateurTest {


    private static String RECEIVER_TOPIC = "kafka-topic-json";

    @Autowired
    private MessageConsommateurLatch messageConsommateur;

    private KafkaTemplate<String, HelloWorld> template;

    @Autowired
    private KafkaEmbedded embeddedKafka;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;


    @BeforeEach
    public void setUp() throws Exception {
        // set up the Kafka producer properties
        Map<String, Object> senderProperties =
                KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());
        senderProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,JsonSerializer.class);

        // create a Kafka producer factory
        ProducerFactory<String, HelloWorld> producerFactory =
                new DefaultKafkaProducerFactory<String, HelloWorld>(senderProperties);

        // create a Kafka template
        template = new KafkaTemplate(producerFactory);
        // set the default topic to send to
        template.setDefaultTopic(RECEIVER_TOPIC);

        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer,
                    embeddedKafka.getPartitionsPerTopic());
        }
    }

    @Test
    public void testReceive() throws Exception {

        sendMessages();

        messageConsommateur.getLatch().await(100, TimeUnit.MILLISECONDS);
        // check that the message was received
        assertThat(messageConsommateur.getLatch().getCount()).isEqualTo(0);
    }

    private void sendMessages() {

        IntStream.range(0,5).forEach(idx -> {
            HelloWorld hw = new HelloWorld("test" + idx,idx);
            template.send(RECEIVER_TOPIC,hw);
            log.info("Message envoyé : {}", hw);
        });
    }


}