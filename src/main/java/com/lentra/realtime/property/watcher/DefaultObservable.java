package com.lentra.realtime.property.watcher;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.lentra.realtime.exception.PropertyObservableFileComparisonException;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

public class DefaultObservable extends AbstractObservable {
    private static final Logger logger= LogManager.getLogger(DefaultObservable.class);

    private static final String PROPERTIES_FILE_PATH = "src/main/resources/mapping.properties";
    private Properties oldProperties = new Properties();

    //CR - Custom Exception to be thrown
    public DefaultObservable() throws IOException {
        loadProperties(oldProperties, PROPERTIES_FILE_PATH);
    }

    private void loadProperties(Properties properties, String filePath) throws IOException {
        try (FileInputStream inStream = new FileInputStream(filePath)) {
            properties.load(inStream);
        }
    }

    @Override
    public void monitorProperties() throws Exception {

        File propertiesFile = new File(PROPERTIES_FILE_PATH);
        FileAlterationObserver observer = new FileAlterationObserver(propertiesFile.getParentFile());
        observer.addListener(new FileAlterationListener() {
            @Override
            public void onStart(FileAlterationObserver fileAlterationObserver) {
            }

            @Override
            public void onDirectoryCreate(File file) {
            }

            @Override
            public void onDirectoryChange(File file) {
            }

            @Override
            public void onDirectoryDelete(File file) {
            }

            @Override
            public void onFileCreate(File file) {
            }

            @Override
            public void onFileChange(File file) {
                if (file.getName().equals(propertiesFile.getName())) {
                    Properties newProperties = new Properties();
                    try {
                        loadProperties(newProperties, PROPERTIES_FILE_PATH);
                        detectPropertyChanges(oldProperties, newProperties);
                        oldProperties = newProperties;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public void onFileDelete(File file) {

            }

            @Override
            public void onStop(FileAlterationObserver fileAlterationObserver) {

            }
        });

        FileAlterationMonitor monitor = new FileAlterationMonitor(TimeUnit.SECONDS.toMillis(1), observer);
        monitor.start();
    }

//    private void detectPropertyChanges2(Properties oldProps, Properties newProps) {
//        Set<String> oldKeys = oldProps.stringPropertyNames();
//        Set<String> newKeys = newProps.stringPropertyNames();
//
//        Set<String> allKeys = new HashSet<>(oldKeys);
//        allKeys.addAll(newKeys);
//
//        Properties deltaProperties = new Properties();
//
//        for (String key : allKeys) {
//            String oldVal = oldProps.getProperty(key);
//            String newVal = newProps.getProperty(key);
//
//            if (isRelevant(key, oldVal, newVal)){
////                logger.info("\nGetting delta and notifying observer"); //comment this out
//                deltaProperties.setProperty(key, newVal);
//                logger.info("Updated properties: " + deltaProperties);
//                notifyObservers(deltaProperties);
//            }
//            else{
//                //logger.info("Not a topic or field level configuration, discarding this change: " + key);
//            }
//
//        }
//    }

    private void detectPropertyChanges(Properties oldProps, Properties newProps) {
        Set<String> oldKeys = oldProps.stringPropertyNames();
        Set<String> newKeys = newProps.stringPropertyNames();

        // NEXT CHANGES: USE HASMAP INSTEAD OF PROPERTIES, FORMAT; Key, ModificationType (Update, Add, Delete), Value (new property if MT=Update or Add, else blank)
        Properties deltaProperties = new Properties();

        for (String key : newKeys) {

            //Incomplete implementation for the rest of this method.
            if (!oldProps.containsKey(key)) {
                System.out.println("New property found: " + key + " = " + newProps.getProperty(key));
                updateRelevantDeltaProperties(key, deltaProperties, newProps);

            } else if (!oldProps.getProperty(key).equals(newProps.getProperty(key))) {
                System.out.println("Property changed: " + key + " (old: " + oldProps.getProperty(key) + ", new: " + newProps.getProperty(key) + ")");
            }
        }

        for (String key : oldKeys) {
            if (!newProps.containsKey(key)) {
                System.out.println("Property removed: " + key);
            }
        }

        notifyObservers(deltaProperties);
    }

    //To be updated to modify HashMap instead of Properties.
    private void updateRelevantDeltaProperties(String key, Properties deltaProperties, Properties newProps) {
        if (key.startsWith("source.topic") || key.startsWith("destination.topic")) {
            deltaProperties.setProperty(key, newProps.getProperty(key));
        } else {
            //logger.info("Not a topic or field level configuration, discarding this change: " + key);
        }
    }

//    private Boolean isRelevantw(String key, String oldVal, String newVal) {
//        if ((oldVal != null && !oldVal.equals(newVal)) || (newVal != null && !newVal.equals(oldVal))) {
//            if (key.startsWith("source.topic") || key.startsWith("destination.topic")) {
//                return true;
//            } else {
//                return false;
//            }
//        }
//        else{
//            return false;
//        }
//    }

//    private Properties getDelta(Properties oldProps, Properties newProps) {
//        Set<String> oldKeys = oldProps.stringPropertyNames();
//        Set<String> newKeys = newProps.stringPropertyNames();
//
//        // Create a new HashSet and add all keys to it to avoid UnsupportedOperationException
//        Set<String> allKeys = new HashSet<>(oldKeys);
//        allKeys.addAll(newKeys);
//
//        Properties deltaProperties = new Properties();
//
//        for (String key : allKeys) {
//            if (newProps.containsKey(key)) {
//                String newVal = newProps.getProperty(key);
//                deltaProperties.setProperty(key, newVal);
//            }
//        }
//        logger.info("Updated properties: " + deltaProperties);
//        return deltaProperties;
//    }


//    private Boolean isRelevant2(Properties oldProps, Properties newProps) {
//        Set<String> oldKeys = oldProps.stringPropertyNames();
//        Set<String> newKeys = newProps.stringPropertyNames();
//
//        // Create a new HashSet and add all keys to it to avoid UnsupportedOperationException
//        Set<String> allKeys = new HashSet<>(oldKeys);
//        allKeys.addAll(newKeys);
//
//        for (String key : allKeys) {
//            String oldVal = oldProps.getProperty(key);
//            String newVal = newProps.getProperty(key);
//
//            if ((oldVal != null && !oldVal.equals(newVal)) || (newVal != null && !newVal.equals(oldVal))) {
//                if (key.startsWith("source.topic") || key.startsWith("destination.topic")) {
//                    continue;
//                } else {
//                    logger.info("Not a topic or field level configuration, discarding this change: " + key);
//                    return false;
//                }
//            }
//        }
//        return true;
//    }


//    private Properties getDelta(Properties oldProps, Properties newProps) {
//        Set<String> oldKeys = oldProps.stringPropertyNames();
//        Set<String> newKeys = newProps.stringPropertyNames();
//
//        // Create a new HashSet and add all keys to it to avoid UnsupportedOperationException
//        Set<String> allKeys = new HashSet<>(oldKeys);
//        allKeys.addAll(newKeys);
//
//        Properties deltaProperties = new Properties();
//
//        for (String key : allKeys) {
//            if (newProps.containsKey(key)) {
//                String newVal = newProps.getProperty(key);
//                deltaProperties.setProperty(key, newVal);
//            }
//        }
//        logger.info("Updated properties: " + deltaProperties);
//        return deltaProperties;
//    }

}