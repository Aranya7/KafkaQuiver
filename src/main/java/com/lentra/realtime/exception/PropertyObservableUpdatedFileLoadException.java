package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class PropertyObservableUpdatedFileLoadException extends IOException {
    private static final Logger logger= LogManager.getLogger(PropertyObservableUpdatedFileLoadException.class);
    private String errorMessage;
    public PropertyObservableUpdatedFileLoadException(String errorMessage) {
        this.errorMessage=errorMessage;
    }
    public void handleException(){
        logger.error(errorMessage);

    }
}
