package com.lentra.realtime.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PipelineConfiguationContentNotFoundException extends Exception{
    private static final Logger logger= LogManager.getLogger(PipelineConfigurationNotFoundException.class);

    public void handleException(){
        logger.error("PipelineConfiguationContentNotFoundException Exception:EmptyPropertiesFile");
        System.exit(0);
    }
}
