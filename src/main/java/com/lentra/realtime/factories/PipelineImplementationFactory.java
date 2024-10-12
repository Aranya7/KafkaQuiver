package com.lentra.realtime.factories;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.PipelineConstants;
import com.lentra.realtime.enums.ImplementationClients;
import com.lentra.realtime.enums.StreamingMedium;
import com.lentra.realtime.pipelines.KStreamPipeline;
import com.lentra.realtime.pipelines.Pipeline;
import com.lentra.realtime.pipelines.impl.PipelineImplementation;
import com.lentra.realtime.pipelines.impl.missioncontrol.MCKStreamImplementation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PipelineImplementationFactory 
{
	 private static final Logger logger=LogManager.getLogger(PipelineImplementationFactory.class);

	 public PipelineImplementation getPipelineImplementation()
	 {
		 logger.info("Get the Kstream implementation provider");
		 return getImplementationClient ()== ImplementationClients.MISSIONCONTROL
				 ? new MCKStreamImplementation()
						 : null;
	 }
	 
	 private ImplementationClients getImplementationClient()
	 {
		 return AppStore.store.getPipelineConfiguration().get(PipelineConstants.PIPELINE_IMPLEMENTATION_CLIENT) != null 
				 ? ImplementationClients.valueOf(AppStore.store.getPipelineConfiguration().get(PipelineConstants.PIPELINE_IMPLEMENTATION_CLIENT)) 
						 : ImplementationClients.MISSIONCONTROL;
	 }
}
