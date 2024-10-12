package com.lentra.realtime.pipelines;

import com.lentra.realtime.exception.*;
import com.lentra.realtime.pipelines.impl.PipelineImplementation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KStreamPipeline extends AbstractRealtimePipeline 
{
	private static final Logger logger=LogManager.getLogger(KStreamPipeline.class);

	public KStreamPipeline(PipelineImplementation implementation)
	{
		super(implementation);
	}
	
	@Override
	public void start() throws PipelineBuildException, PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException {
		logger.info("Starting the pipeline");


		getImplementation().initialize(getSourceEndPoint(), getDestinationEndPoint(), this);
		getImplementation().implement();
	}

	@Override
	public void stop() 
	{
		// TODO Auto-generated method stub
	}
}