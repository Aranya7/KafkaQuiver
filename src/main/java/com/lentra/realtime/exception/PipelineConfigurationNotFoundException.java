package com.lentra.realtime.exception;

import com.lentra.realtime.MCKStreamRealtime.App;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.util.IO;

import java.io.FileNotFoundException;
import java.io.IOException;

public class PipelineConfigurationNotFoundException extends IOException{
    private static final Logger logger= LogManager.getLogger(PipelineConfigurationNotFoundException.class);
    public void handleException(){
        logger.error("PipelineConfigurationNotFoundException Exception:FileNotFound");

    }

}