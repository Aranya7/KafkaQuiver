package com.lentra.realtime.factories;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.exception.NoPropertryReaderFoundError;

import com.lentra.realtime.property.watcher.DefaultObservable;
import com.lentra.realtime.property.watcher.JSONObservable;
import com.lentra.realtime.property.watcher.PropertiesObservable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public abstract class ObservableFactory {
    private static final Logger logger = LogManager.getLogger(ObservableFactory.class);

    //CR - IO Exception to be handled
    public static PropertiesObservable getObservable() throws IOException {
        String pathType = AppStore.store.getPropertiesFileFormat();
        if (pathType.contains(".json")) {
            return new JSONObservable();
        } else if (pathType.contains(".properties")) {
            return new DefaultObservable();
        }
        else {
            logger.error("No Reader implementation for extension identified : {}",
                    pathType == null || !(pathType.contains(".")) ? "" : pathType.split(".")[1]);
            throw new NoPropertryReaderFoundError("Failed in identifying the required reader implementation.");
    }

    }
}
