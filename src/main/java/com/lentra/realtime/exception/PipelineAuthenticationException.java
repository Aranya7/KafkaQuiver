package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PipelineAuthenticationException extends Exception{
    private String errorMessage;
    private static final Logger logger= LogManager.getLogger(PipelineAuthenticationException.class);
    public  PipelineAuthenticationException(String errorMessage)
    {
        this.errorMessage=errorMessage;
    }

    public void handleException(){
        logger.error("Error occured in reading Authentication="+errorMessage);
        System.exit(0);
    }
}
