package com.lentra.realtime.factories;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.PipelineConstants;
import com.lentra.realtime.enums.StreamingMedium;
import com.lentra.realtime.pipelines.KStreamPipeline;
import com.lentra.realtime.pipelines.Pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PipelineFactory 
{
	private static final Logger logger=LogManager.getLogger(PipelineFactory.class);

	private PipelineImplementationFactory implementationFactory ;
	
	public PipelineFactory()
	{
		implementationFactory = new PipelineImplementationFactory();
	}
	
	 public Pipeline getPipeline()
	 {
		 logger.info("Identify the implementation medium of streaming source data.");
		 logger.debug("The streaming medium is {}", getStreamingMedium());
		 return getStreamingMedium()== StreamingMedium.KSTREAM 
				 ? new KStreamPipeline(implementationFactory.getPipelineImplementation()) 
						 : null;
	 }
	 
	 private StreamingMedium getStreamingMedium()
	 {
		 return AppStore.store.getPipelineConfiguration().get(PipelineConstants.PIPELINE_STREAMING_MEDIUM) != null 
				 ? StreamingMedium.valueOf(AppStore.store.getPipelineConfiguration().get(PipelineConstants.PIPELINE_STREAMING_MEDIUM)) 
						 : StreamingMedium.KSTREAM;
	 }
}
