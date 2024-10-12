package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataConverterException extends Exception{

    private static final Logger logger= LogManager.getLogger(DataConverterException.class);
    String errorMessage;
    public DataConverterException(String errorMesssage){
        this.errorMessage=errorMesssage;
    }


    public void handleException(){
        logger.error("Exception occured during data type conversion:"+errorMessage);
        System.exit(0);
    }
    public void handelDataConverterException(){
        logger.error("Exception Occured while data type conversion");
        System.exit(0);
    }
}
