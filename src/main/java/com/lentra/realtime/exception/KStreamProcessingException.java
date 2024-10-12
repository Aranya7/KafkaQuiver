package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;

public class KStreamProcessingException extends Exception{
    private static final Logger logger= LogManager.getLogger(KStreamProcessingException.class);

    String errorMessage;

    public KStreamProcessingException(String erroMessage) {
        this.errorMessage=erroMessage;
    }

    public KStreamProcessingException(JSONException e) {
        this.errorMessage= String.valueOf(e);
    }

    public KStreamProcessingException(IndexOutOfBoundsException e) {
        this.errorMessage= String.valueOf(e);
    }





    public void handleException(){

        logger.error("Exception Occured while getting after: "+errorMessage);

    }


}
