package ch.globaz.devsecops.kafka.consommateur.config;


import ch.globaz.devsecops.kafka.common.HelloWorld;
import ch.globaz.devsecops.kafka.consommateur.consommateur.MessageConsommateur;
import ch.globaz.devsecops.kafka.consommateur.producteur.MessageProducteur;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonDeserializer;


import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsommateurConfig {

    private final String GROUP_NAME  = "test-kafka-group2";

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    public ConsumerFactory<String, HelloWorld> consumerFactory() {

        final JsonDeserializer<HelloWorld> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("ch.globaz.devsecops.kafka.common");


        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE);
        //props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,"ch.globaz.devsecops.kafka.common.HelloWorld");
        //props.put(JsonDeserializer.TRUSTED_PACKAGES, "ch.globaz.devsecops.kafka.common");

        final DefaultKafkaConsumerFactory<String, HelloWorld> defaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(props);

        defaultKafkaConsumerFactory.setKeyDeserializer(new StringDeserializer());
        defaultKafkaConsumerFactory.setValueDeserializer(jsonDeserializer);

        return defaultKafkaConsumerFactory;
    }


    @Bean
    public MessageConsommateur messageConsommateur() {
        return new MessageConsommateur();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, HelloWorld> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, HelloWorld> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setAckOnError(Boolean.FALSE);

        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    KafkaListenerErrorHandler errorHandler() {
        return (message, e) -> {
            System.out.println(String.format("error handler for message: %s [%s], exception: %s", message.getPayload(), message.getHeaders(), e.getMessage()));
            MessageListenerContainer listenerContainer = registry.getListenerContainer("listener");
            messageProducer().sendMessage((HelloWorld) message.getPayload());
            return null;
        };
    }

    @Autowired
    KafkaListenerEndpointRegistry registry;


    @Bean
    public MessageProducteur messageProducer() {
        return new MessageProducteur();
    }


}
