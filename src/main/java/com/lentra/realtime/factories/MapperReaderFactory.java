package com.lentra.realtime.factories;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.lentra.realtime.exception.NoPropertryReaderFoundError;
import com.lentra.realtime.mappers.JsonFileMapperReader;
import com.lentra.realtime.mappers.MapperReader;
import com.lentra.realtime.mappers.PropertyFileMapperReader;

public abstract class MapperReaderFactory {

	private static final Logger logger = LogManager.getLogger(MapperReaderFactory.class);

	public static MapperReader getMapperReader(String path) throws NoPropertryReaderFoundError{
		logger.info("Identifying the property reader implementation.");
		Optional<String> optionalPath = Optional.ofNullable(path);
		if (optionalPath.isPresent()) {
			if (path.contains(".json")) {
				logger.debug("File extension identified : JSON");
				logger.debug("Returning instance of JSON configuration reader");
				return new JsonFileMapperReader();
			} else if (path.contains(".properties")) {
				logger.debug("File extension identified : Properties");
				logger.debug("Returning instance of Property File reader");
				return new PropertyFileMapperReader(path);
			}
		} else {
			logger.error("No Reader implementation for extension identified : {}",
					path == null || !(path.contains(".")) ? "" : path.split(".")[1] );
		}
		throw new NoPropertryReaderFoundError("Failed in identifying the required reader implementation.");
	}
}