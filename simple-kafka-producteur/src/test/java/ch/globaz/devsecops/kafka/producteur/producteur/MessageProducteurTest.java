package ch.globaz.devsecops.kafka.producteur.producteur;

import ch.globaz.devsecops.kafka.common.HelloWorld;
import ch.globaz.devsecops.kafka.producteur.config.KafkaProducteurConfig;
import ch.globaz.devsecops.kafka.producteur.producteur.MessageProducteur;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;


/**
 * Created by seb on .
 * <p>
 * ${VERSION}
 */
@Slf4j
@ExtendWith(SpringExtension.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = "${kafka.topic.test.name}" )
@SpringBootTest(classes = {MessageProducteur.class,KafkaProducteurConfig.class})
@ActiveProfiles("test")
class MessageProducteurTest {

    @Value("${kafka.bootstrapAddress}")
    private String brokerAddresses;

    @Value("${kafka.topic.test.name}")
    private String topic;

    @Value("${kafka.topic.test.consumer-group.name}")
    private String groupName;

    private BlockingQueue<ConsumerRecord<String, HelloWorld>> records;

    private KafkaMessageListenerContainer<String, HelloWorld> container;

    @Autowired
    private KafkaEmbedded embeddedKafka;

    @Autowired
    private MessageProducteur messageProducteur;


    @BeforeEach
    public void setUp() throws Exception {

        log.info("bootstrapAddress: {}",brokerAddresses);
        log.info("topic: {}",topic);
        log.info("groupName: {}",groupName);

        // définition de propriétés du consommateur
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps(groupName, "false", embeddedKafka);

        // Création du consummer factory
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<String, String>(consumerProperties);

        // Définition du topic sur lequel consommer
        ContainerProperties containerProperties = new ContainerProperties(topic);

        // Création du container d'écoute
        container = new KafkaMessageListenerContainer(consumerFactory, containerProperties);

        // Instanciation de la queue thread safe afin de stocker les messages
        records = new LinkedBlockingQueue<>();

        // définition du listener
        container.setupMessageListener(new MessageListener<String, HelloWorld>() {
            @Override
            public void onMessage(ConsumerRecord<String, HelloWorld> record) {
                log.info("consummer onMessage : {}",record);
                records.add(record);
            }
        });

        // démarrage du container et de la consommation
        container.start();

        //attente sur le container afin que les partitions requises soient assignées
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }


    @Test
    public void testSend() throws InterruptedException {



        // send the message
        messageProducteur.sendMessage(new HelloWorld("premier",1));
        messageProducteur.sendMessage(new HelloWorld("second",2));
        messageProducteur.sendMessage(new HelloWorld("troisieme",3));

        // check that the message was received
        ConsumerRecord<String, HelloWorld> received1 = records.poll(5, TimeUnit.SECONDS);
        ConsumerRecord<String, HelloWorld> received2 = records.poll(5, TimeUnit.SECONDS);
        ConsumerRecord<String, HelloWorld> received3 = records.poll(5, TimeUnit.SECONDS);

        System.out.println("rec" + received1);

        // Hamcrest Matchers to check the value
        assertThat(received1.value()).isNotNull();
        assertThat(received2).isNotNull();
        assertThat(received3).isNotNull();
        // AssertJ Condition to check the key
        assertThat(received1).has(key(null));
        assertThat(received2).has(key(null));
        assertThat(received3).has(key(null));
    }

    @AfterEach
    public void tearDown() {
        // stop the container
        container.stop();
    }
}