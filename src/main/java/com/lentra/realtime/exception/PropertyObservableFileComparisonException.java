package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PropertyObservableFileComparisonException extends Exception{
    private static final Logger logger= LogManager.getLogger(PropertyObservableFileComparisonException.class);
    private String errorMessage;
    public PropertyObservableFileComparisonException(String errorMessage) {
        this.errorMessage=errorMessage;
    }

    public void handleException(){
        logger.error(errorMessage);

    }
}
