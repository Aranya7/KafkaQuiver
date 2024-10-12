package com.lentra.realtime.mappers;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.lentra.realtime.exception.PipelineConfiguationContentNotFoundException;
import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.lentra.realtime.factories.MapperReaderFactory;

public class PropertyFileMapperReader implements MapperReader {

	private static final Logger logger = LogManager.getLogger(PropertyFileMapperReader.class);

	private String propertyFilePath;

	public PropertyFileMapperReader(String propertyFilePath) {
		this.propertyFilePath = propertyFilePath;
	}

	@Override
	public Optional<Map<String, String>> getPipelineConfiguration()
			throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException {

		logger.info("Building the configuration store");

		if (propertyFilePath == null || propertyFilePath.isEmpty()) {
			throw new PipelineConfigurationNotFoundException();

		}
		HashMap<String, String> configMap = new HashMap<>();
		Properties properties = new Properties();

		try (FileInputStream inputStream = new FileInputStream(propertyFilePath)) {
			properties.load(inputStream);
		} catch (IOException e) {
			logger.error("Failure in loading the content of input configuration.");
			throw new PipelineConfigurationNotFoundException();
		}
		if (properties.isEmpty()) {
			logger.error("Empty configuration provided");
			throw new PipelineConfiguationContentNotFoundException();
		}

		for (String key : properties.stringPropertyNames()) {
			String value = properties.getProperty(key);
			configMap.put(key, value);
		}
		logger.info("Successfully imported all the configuations");

		return Optional.of(configMap);
	}
}
