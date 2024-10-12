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

	public ConfigStoreBuilder(String configPath)
			throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException {
		MapperReader configReaderObject = MapperReaderFactory.getMapperReader(configPath);
		configMap = configReaderObject.getPipelineConfiguration();
	}

	public Store build() {
		return new ConfigStore(configMap);
	}

	private class ConfigStore extends AbstractConfigStore {
		private Map<String, String> pipelineConfiguration;
		private Map<String, String> sourceEndPointConfiguration;
		private Map<String, String> destinationEndPointConfiguration;
		private Map<String, String> sourceEndPointTopicConfiguration;
		private Map<String, String> destinationEndPointTopicConfiguration;

		public ConfigStore(Optional<Map<String, String>> configMap) {
			this.userDefinedConfiguration = configMap;
			pipelineConfiguration = buildPipelineConfiguration();
			sourceEndPointConfiguration = buildSourceEndPointConfiguration();
			destinationEndPointConfiguration = buildDestinationEndPointConfiguration();
			sourceEndPointTopicConfiguration = buildSourceTopicConfiguration();
			destinationEndPointTopicConfiguration = buildDestinationTopicConfiguration();
		}

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
	}

}
