package com.lentra.realtime.property.watcher;

import java.util.Properties;

public interface PropertiesObservable {
    void registerObserver(PropertiesObserver observer);
    void notifyObservers(Properties newProperties);
    void monitorProperties() throws Exception;
}
