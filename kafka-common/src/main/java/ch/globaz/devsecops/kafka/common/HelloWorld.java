package ch.globaz.devsecops.kafka.common;

import java.util.Date;

public class HelloWorld {

    private String message;
    private int numero;
    private long timestamp;

    public HelloWorld(){}

    public String getMessage() {
        return message;
    }

    public int getNumero() {
        return numero;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "HelloWorld{" +
                "message='" + message + '\'' +
                ", numero=" + numero +
                ", timestamp=" + timestamp +
                '}';
    }

    public HelloWorld(String message, int numero){
        this.message = message;
        this.numero  = numero;
        this.timestamp = new Date().getTime();
    }
}
