package ch.globaz.devsecops.kafka.simpleStringProducer;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Application produisatnt des messages sur un broker kafka à intervalle régulier
 */
@SpringBootApplication
@EnableScheduling
public class SimpleKafkaProducteurApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimpleKafkaProducteurApplication.class);
    }


}
