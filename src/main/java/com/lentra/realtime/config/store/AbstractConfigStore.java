package com.lentra.realtime.config.store;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import com.lentra.realtime.constants.EndPointConstants;
import com.lentra.realtime.constants.PipelineConstants;
import com.lentra.realtime.constants.TopicConstants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractConfigStore implements Store
{
	private static final Logger logger=LogManager.getLogger(AbstractConfigStore.class);

	public Optional<Map<String,String>> userDefinedConfiguration;

	@Override
	public final Map<String, String> buildPipelineConfiguration() {
		logger.info("Building Pipeline configurations");
		return userDefinedConfiguration.isPresent() ? Collections.unmodifiableMap(userDefinedConfiguration.get().entrySet().stream()
				.filter(x -> x.getKey().startsWith(PipelineConstants.PIPELINE_PREFIX))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))) : null;

	}

	@Override
	public final Map<String, String> buildSourceEndPointConfiguration() {
		logger.info("Building Source end point configurations");
		return userDefinedConfiguration.isPresent() ? Collections.unmodifiableMap(userDefinedConfiguration.get().entrySet().stream()
				.filter(x -> x.getKey().startsWith(EndPointConstants.SOURCE_PREFIX))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))) : null;
	}

	@Override
	public final Map<String, String> buildDestinationEndPointConfiguration() {
		logger.info("Building Destination end point configurations");
		return userDefinedConfiguration.isPresent() ? Collections.unmodifiableMap(userDefinedConfiguration.get().entrySet().stream()
				.filter(x -> x.getKey().startsWith(EndPointConstants.DESTINATION_PREFIX))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))) : null;
	}

	@Override
	public final Map<String, String> buildSourceTopicConfiguration() {
		logger.info("Building Source topics configurations");
		return userDefinedConfiguration.isPresent() ? Collections.unmodifiableMap(userDefinedConfiguration.get().entrySet().stream()
				.filter(x -> x.getKey().startsWith(TopicConstants.SOURCE_TOPIC_PREFIX))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))) : null;
	}

	@Override
	public final Map<String, String> buildDestinationTopicConfiguration() {
		logger.info("Building Destination topics configurations");
		return userDefinedConfiguration.isPresent() ? Collections.unmodifiableMap(userDefinedConfiguration.get().entrySet().stream()
				.filter(x -> x.getKey().startsWith(TopicConstants.DESTINATION_TOPIC_PREFIX))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))) : null;
	}

	@Override
	public final Map<String, String> buildUserMapperConfiguration() {
		return userDefinedConfiguration.isPresent() ? Collections.unmodifiableMap(userDefinedConfiguration.get()) : null;
	}

	public void setConfigurationMap(Optional<Map<String, String>> userDefinedConfiguration) {
		this.userDefinedConfiguration = userDefinedConfiguration;
	}
}
