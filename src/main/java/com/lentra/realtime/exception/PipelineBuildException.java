package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PipelineBuildException extends Exception{
    String errorMessage;
    private static final Logger logger= LogManager.getLogger(PipelineBuildException.class);

    public PipelineBuildException(String errorMessage){
        this.errorMessage=errorMessage;
    }

    public void handleException(){
        logger.error("Error occured in buidling Kstream="+errorMessage);
        System.exit(0);
    }
}
