package ch.globaz.devsecops.kafka.simpleStringProducer.process;

import ch.globaz.devsecops.kafka.simpleStringProducer.producteur.MessageProducteur;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * Process async de production de message, schedulé par Spring
 */
@Component
public class MessageProductionProcess {

    private MessageProducteur producteur;

    private int compteurMessage = 0;

    @Autowired
    public MessageProductionProcess(MessageProducteur producteur){
        this.producteur = producteur;
    }

    @Scheduled(fixedRate = 250)
    public void startProduction(){
        compteurMessage ++;
        producteur.sendMessage(String.format("Hello World, message no: %d,timestamp: %d", compteurMessage, new Date().getTime()));
        System.out.println("Sending message, no " + compteurMessage);
    }
}
