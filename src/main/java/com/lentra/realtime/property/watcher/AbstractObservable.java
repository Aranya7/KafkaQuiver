package com.lentra.realtime.property.watcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class AbstractObservable implements PropertiesObservable {
    private List<PropertiesObserver> observers = new ArrayList<>();

    @Override
    public void registerObserver(PropertiesObserver observer) {
        observers.add(observer);
    }

    @Override
    public void notifyObservers(Properties newProperties) {
        for (PropertiesObserver observer : observers) {
            observer.update(newProperties);
        }
    }
}
