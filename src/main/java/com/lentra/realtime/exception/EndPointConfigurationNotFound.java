package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EndPointConfigurationNotFound extends Exception{
    private static final Logger logger= LogManager.getLogger(EndPointConfigurationNotFound.class);
    String errorMessage;
    public EndPointConfigurationNotFound(String errorMesssage){
        this.errorMessage=errorMesssage;
    }


    public void handleException(){
        logger.error("Exception Occured while getting endpoints configurations:"+errorMessage);
        System.exit(0);
    }

}
