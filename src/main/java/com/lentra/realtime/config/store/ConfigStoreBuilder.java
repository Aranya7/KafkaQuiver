package com.lentra.realtime.config.store;

import java.util.Map;
import java.util.Optional;
import com.lentra.realtime.exception.PipelineConfiguationContentNotFoundException;
import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;
import com.lentra.realtime.factories.MapperReaderFactory;
import com.lentra.realtime.mappers.MapperReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConfigStoreBuilder {

	private static final Logger logger = LogManager.getLogger(ConfigStoreBuilder.class);

	private Optional<Map<String, String>> configMap;
	private MapperReader configReaderObject;
	private ConfigStore configStore;
	private static String propertyFileFormat;

	public ConfigStoreBuilder(String configPath)
			throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException {
		MapperReaderFactory mapperReaderFactory = new MapperReaderFactory();
		configReaderObject = mapperReaderFactory.getMapperReader(configPath);
		propertyFileFormat = mapperReaderFactory.getPropertyFileFormat();
		configMap = configReaderObject.getPipelineConfiguration();
	}

	public Store build() {
		configStore = new ConfigStore(configMap, propertyFileFormat);
		return configStore;
	}

	public void update() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException {
		Optional<Map<String, String>> updatedConfigMap = configReaderObject.getPipelineConfiguration();
		configStore.updateUserDefinedConfiguration(updatedConfigMap);
		configStore.updateEndPointTopicConfiguration();
	}

	private class ConfigStore extends AbstractConfigStore {
		private Map<String, String> pipelineConfiguration;
		private Map<String, String> sourceEndPointConfiguration;
		private Map<String, String> destinationEndPointConfiguration;
		private Map<String, String> sourceEndPointTopicConfiguration;
		private Map<String, String> destinationEndPointTopicConfiguration;
		private String propertiesFileFormat;

		public ConfigStore(Optional<Map<String, String>> configMap, String propertyFileFormat) {
			this.userDefinedConfiguration = configMap;
			pipelineConfiguration = buildPipelineConfiguration();
			sourceEndPointConfiguration = buildSourceEndPointConfiguration();
			destinationEndPointConfiguration = buildDestinationEndPointConfiguration();
			sourceEndPointTopicConfiguration = buildSourceTopicConfiguration();
			destinationEndPointTopicConfiguration = buildDestinationTopicConfiguration();
			propertiesFileFormat = propertyFileFormat;
		}

		//public ConfigStore(){}

		public Map<String, String> getPipelineConfiguration() {
			return pipelineConfiguration;
		}

		public Map<String, String> getSourceEndPointConfiguration() {
			return sourceEndPointConfiguration;
		}

		public Map<String, String> getDestinationEndPointConfiguration() {
			return destinationEndPointConfiguration;
		}

		public Map<String, String> getSourceEndPointTopicConfiguration() {
			return sourceEndPointTopicConfiguration;
		}

		public Map<String, String> getDestinationEndPointTopicConfiguration() {
			return destinationEndPointTopicConfiguration;
		}

		public String getPropertiesFileFormat() {
			return propertiesFileFormat;
		}

		public void updateUserDefinedConfiguration(Optional<Map<String, String>> updateUserDefinedConfiguration) {
			this.userDefinedConfiguration = updateUserDefinedConfiguration;
		}

		public void updateEndPointTopicConfiguration(){
			updateSourceEndPointTopicConfiguration();
			updateDestinationEndPointTopicConfiguration();
		}

		public void updateSourceEndPointTopicConfiguration(){
			sourceEndPointTopicConfiguration = buildSourceTopicConfiguration();
		}

		public void updateDestinationEndPointTopicConfiguration(){
			destinationEndPointTopicConfiguration = buildDestinationTopicConfiguration();
		}

	}

}
