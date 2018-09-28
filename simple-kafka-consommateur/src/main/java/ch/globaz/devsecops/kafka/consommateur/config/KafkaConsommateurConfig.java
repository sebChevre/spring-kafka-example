package ch.globaz.devsecops.kafka.consommateur.config;


import ch.globaz.devsecops.kafka.common.HelloWorld;
import ch.globaz.devsecops.kafka.consommateur.consommateur.MessageConsommateur;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;


import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsommateurConfig {

    private final String GROUP_NAME  = "test-kafka-group";

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    public ConsumerFactory<String, HelloWorld> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,"ch.globaz.devsecops.kafka.common.HelloWorld");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ch.globaz.devsecops.kafka.common");
        return new DefaultKafkaConsumerFactory(props, new StringDeserializer(), new JsonDeserializer<HelloWorld>());
    }


    @Bean
    public MessageConsommateur messageConsommateur() {
        return new MessageConsommateur();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, HelloWorld> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, HelloWorld> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }




}
