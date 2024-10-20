package com.lentra.realtime.property.watcher;

import java.util.Properties;

public interface PropertiesObserver {
    void update(Properties newProperties);
    //void update(File updatedFile);
}
