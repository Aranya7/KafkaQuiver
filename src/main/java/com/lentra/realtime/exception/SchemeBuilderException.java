package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemeBuilderException extends Exception{

    private static final Logger logger= LogManager.getLogger(SchemeBuilderException.class);
    String errorMessage;
    public SchemeBuilderException(String errorMesssage){
        this.errorMessage=errorMesssage;
    }


    public void handleException(){
        logger.error("Exception occured during data type conversion:"+errorMessage);
        System.exit(0);
    }

}
