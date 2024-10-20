package com.lentra.realtime.pipelines;

import com.lentra.realtime.exception.*;
import com.lentra.realtime.factories.ObservableFactory;
import com.lentra.realtime.pipelines.impl.PipelineImplementation;

import com.lentra.realtime.property.watcher.PropertiesObservable;
import com.lentra.realtime.property.watcher.PropertiesObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

public class KStreamPipeline extends AbstractRealtimePipeline 
{
	private static final Logger logger=LogManager.getLogger(KStreamPipeline.class);

	public KStreamPipeline(PipelineImplementation implementation)
	{
		super(implementation);
	}
	
	@Override
	public void start() throws PipelineBuildException, PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException, PropertyObservableUpdatedFileLoadException, PropertyObservableFileComparisonException {
		logger.info("Starting the pipeline");

		getImplementation().initialize(getSourceEndPoint(), getDestinationEndPoint(), this);
		getImplementation().implement();

		PropertiesObservable observable;
		try{
			observable = ObservableFactory.getObservable();
		} catch (IOException e) {
			throw new PropertyObservableUpdatedFileLoadException("Error Loading/Reading updated properties file");
		}

		observable.registerObserver(this);
		try {
			observable.monitorProperties();
		} catch (Exception e) {
			throw new PropertyObservableFileComparisonException("Error Monitoring changed properties file");
		}




	}

	@Override
	public void stop() 
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void update(Properties newProperties) {
		getImplementation().pause();
		// Source and Destination Topics and Fields to be updated here.
		getImplementation().resume();
	}
}