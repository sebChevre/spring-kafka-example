package ch.globaz.devsecops.kafka.producteur;

import ch.globaz.devsecops.kafka.common.HelloWorld;
import ch.globaz.devsecops.kafka.producteur.config.KafkaProducteurConfig;
import ch.globaz.devsecops.kafka.producteur.producteur.MessageProducteur;
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
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by seb on .
 * <p>
 * ${VERSION}
 */
@ExtendWith(SpringExtension.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "kafka-topic-json" })
@SpringBootTest(classes = {MessageProducteur.class,KafkaProducteurConfig.class})
class SimpleKafkaProducteurApplicationTest {

    @Value("${" + KafkaEmbedded.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
    private String brokerAddresses;

    private BlockingQueue<ConsumerRecord<String, HelloWorld>> records;

    private KafkaMessageListenerContainer<String, HelloWorld> container;


    @Autowired
    private KafkaEmbedded embeddedKafka;

    @Autowired
    private MessageProducteur messageProducteur;

    @Test
    public void test() {
        // fail("Not yet implemented");
        System.out.println(brokerAddresses);
    }

    @BeforeEach
    public void setUo() throws Exception {
        // set up the Kafka consumer properties
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("sender", "false", embeddedKafka);

        // create a Kafka consumer factory
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<String, String>(consumerProperties);

        // set the topic that needs to be consumed
        ContainerProperties containerProperties = new ContainerProperties("kafka-topic-json");

        // create a Kafka MessageListenerContainer
        container = new KafkaMessageListenerContainer(consumerFactory, containerProperties);

        // create a thread safe queue to store the received message
        records = new LinkedBlockingQueue<>();

        // setup a Kafka message listener
        container.setupMessageListener(new MessageListener<String, HelloWorld>() {
            @Override
            public void onMessage(ConsumerRecord<String, HelloWorld> record) {
                System.out.println("test-listener received message='{" + record.value() +"}");
                records.add(record);
            }
        });

        // start the container and underlying message listener
        container.start();

        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }


    @Test
    public void testSend() throws InterruptedException {



        // send the message
        String greeting = "Hello Spring Kafka Sender!";
        messageProducteur.sendMessage(new HelloWorld());

        // check that the message was received
        ConsumerRecord<String, HelloWorld> received = records.poll(1, TimeUnit.SECONDS);

        System.out.println("rec" + received);
        // Hamcrest Matchers to check the value
        //assertThat(received, hasValue(greeting));
        // AssertJ Condition to check the key
        //assertThat(received).has(key(null));
    }

    @AfterEach
    public void tearDown() {
        // stop the container
        container.stop();
    }
}