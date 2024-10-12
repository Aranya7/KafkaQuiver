package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

public class GetDestinationValueException extends Exception{

    private static final Logger logger= LogManager.getLogger(GetDestinationValueException.class);
    String errorMessage;
    public GetDestinationValueException(String errorMesssage){
        this.errorMessage=errorMesssage;
    }

    public GetDestinationValueException(JSONObject json) {
    }


    public void handleException(){
        logger.error("Exception Occured while processing destination value:"+errorMessage);
        System.exit(0);
    }

}
