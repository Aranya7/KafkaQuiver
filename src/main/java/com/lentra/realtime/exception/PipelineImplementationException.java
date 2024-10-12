package com.lentra.realtime.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PipelineImplementationException extends Exception {

    String errorMessage;
    private static final Logger logger= LogManager.getLogger(PipelineImplementationException.class);
    public PipelineImplementationException(String errorMessage){
        this.errorMessage=errorMessage;
    }

    public void handleException(){
        logger.error("Error occured in Implementation="+errorMessage);
        System.exit(0);
    }


}
