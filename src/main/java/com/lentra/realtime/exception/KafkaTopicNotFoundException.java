package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaTopicNotFoundException extends Exception {

    private static final Logger logger= LogManager.getLogger(KafkaTopicNotFoundException.class);
    String errorMessage;
    public KafkaTopicNotFoundException(String errorMesssage){
        this.errorMessage=errorMesssage;
    }


    public void handleException(){
        logger.error("Kafka topic not found:"+errorMessage);
        System.exit(0);
    }
}
