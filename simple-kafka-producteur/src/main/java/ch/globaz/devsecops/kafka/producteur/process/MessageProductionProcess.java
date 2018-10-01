package ch.globaz.devsecops.kafka.producteur.process;


import ch.globaz.devsecops.kafka.common.HelloWorld;
import ch.globaz.devsecops.kafka.producteur.producteur.MessageProducteur;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;



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
        if(compteurMessage < 20){
            compteurMessage ++;
            HelloWorld hw = new HelloWorld("Hello world", compteurMessage);
            producteur.sendMessage(hw);
            System.out.println("Sending message: " + hw);
        }else{
            System.exit(0);
        }

    }
}
