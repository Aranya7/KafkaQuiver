package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PipelineInitializationException extends Exception{
    private String errorMessage;
    public PipelineInitializationException(String errorMessage){
        this.errorMessage=errorMessage;
    }

    public PipelineInitializationException(){

    }

    public void handleException(){
        logger.error("Error occured in reading property : {}", errorMessage);
        System.exit(0);
    }

    public void handleFieldException(){
        System.exit(0);
    }


    private static final Logger logger= LogManager.getLogger(PipelineInitializationException.class);

}