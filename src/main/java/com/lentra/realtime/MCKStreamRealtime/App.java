package com.lentra.realtime.MCKStreamRealtime;

import com.lentra.realtime.config.store.*;
import com.lentra.realtime.exception.*;
import com.lentra.realtime.factories.PipelineFactory;
import com.lentra.realtime.factories.PipelineImplementationFactory;
import com.lentra.realtime.pipelines.Pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class App {
	private static final Logger logger = LogManager.getLogger(App.class);

	public static void main(String[] args) {
		try {
			logger.info("Start of getting all the configurations");

			String filePathOfMapping = args[0];  // Validate this input

			if(filePathOfMapping==null || filePathOfMapping.isEmpty()){
				logger.error("No file Path provided");
				System.exit(0);
			}
			
//			String filePathOfMapping = "/home/aranyasinghchauhan/Videos/MC_Repo_New/insights-reports/New_MCKStreamRealtime/MCKStreamRealtime/src/main/resources/mapping.properties";
			logger.info("Initiating the config store builder module.");
			AppStore.store = new ConfigStoreBuilder(filePathOfMapping).build();
			logger.info("Store builder module completed successfully. Initiating the process to build the streaming pipeline.");
			logger.debug("App Store : {}", AppStore.store);

			PipelineFactory factory = new PipelineFactory();
			Pipeline pipeline = factory.getPipeline();
			logger.info("Builder process of streaming pipeline object is completed sucessfully");
			logger.debug("Pipeline Configuration : {}", pipeline);

			logger.info("Starting the phase for pipeline initialization.");
			pipeline.initialize();
			pipeline.start();

		} catch (PipelineConfigurationNotFoundException e) {
			e.handleException();

		} catch (PipelineConfiguationContentNotFoundException e) {
			e.handleException();

		} catch (PipelineInitializationException e) {
			e.handleException();

		} catch (PipelineBuildException e) {
			e.handleException();
		} catch (PipelineAuthenticationException e) {
			e.handleException();
		} catch (EndPointConfigurationNotFound e) {
			e.handleException();
		} catch (KafkaTopicNotFoundException e) {
			e.handleException();
		} catch (PropertyObservableUpdatedFileLoadException e) {
			e.handleException();
        } catch (PropertyObservableFileComparisonException e) {
			e.handleException();
        }
    }

}
