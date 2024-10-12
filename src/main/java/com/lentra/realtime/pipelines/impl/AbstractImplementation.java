package com.lentra.realtime.pipelines.impl;

import java.util.List;
import java.util.Properties;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.PipelineConstants;
import com.lentra.realtime.entity.endpoint.SourceEndPoint;
import com.lentra.realtime.enums.OffsetPositions;
import com.lentra.realtime.exception.*;
import com.lentra.realtime.handler.datatypes.DataTypeConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.json.JSONObject;

import com.lentra.realtime.entity.Field.Field;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.entity.endpoint.EndPoint;
import com.lentra.realtime.factories.DataTypeConversionFactory;
import com.lentra.realtime.factories.SchemaFactory;
import com.lentra.realtime.pipelines.Pipeline;
import com.lentra.realtime.utils.SchemaUtils;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractImplementation implements PipelineImplementation {
	private static final Logger logger = LogManager.getLogger(AbstractImplementation.class);

	private StreamsBuilder builder;

	protected EndPoint source;
	protected EndPoint target;
	protected Pipeline pipeline;

	protected Producer<String, String> producer;
	private static final DataTypeConversionFactory dataTypeConversionFactory = new DataTypeConversionFactory();

	@Override
	public void initialize(EndPoint source, EndPoint target, Pipeline pipeline)
			throws EndPointConfigurationNotFound, PipelineAuthenticationException {
		this.builder = new StreamsBuilder();
		this.source = source;
		this.target = target;
		this.pipeline = pipeline;
		producer = new KafkaProducer<>(configureDestinationEndPointForStreaming());
	}

	public Properties configureDestinationEndPointForStreaming()
			throws PipelineAuthenticationException, EndPointConfigurationNotFound {
		logger.info("Configuring streaming for Destination Kafka");
		Properties kafkaDestinationConfigs = new Properties();
		try {
			kafkaDestinationConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, target.getBootstrapURL());
		} catch (Exception e) {
			throw new EndPointConfigurationNotFound(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

		}
		kafkaDestinationConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaDestinationConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		if (target.getIsAuthenticationEnabled()) {
			authenticateEndPointKafka(kafkaDestinationConfigs, target);
		}
		return kafkaDestinationConfigs;
	}

	private void authenticateEndPointKafka(Properties kafkaConfigs, EndPoint endPoint)
			throws PipelineAuthenticationException {
		endPoint.getAuthenticator().addAuthenticationConfig(kafkaConfigs, endPoint.getEndPointType());
	}

	@Override
	public KafkaStreams getKafkaStream(List<String> topicList) throws PipelineBuildException, PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException {
		if (topicList==null || topicList.isEmpty()){
			throw new KafkaTopicNotFoundException("Empty or null topic list");
		}
		buildTopicStream(topicList);
		return new KafkaStreams(builder.build(), configureSourceEndPointForStreaming());
	}

	private void buildTopicStream(List<String> topicList) throws PipelineBuildException, KafkaTopicNotFoundException {

		for (String topic : topicList) {
			SourceEndPoint sourceEndPoint = (SourceEndPoint) pipeline.getSourceEndPoint();
			String topicName = topic.substring(topic.lastIndexOf('.') + 1);

			logger.info("Building topic stream for {}", topicName);
			Topic topicObj = sourceEndPoint.getTopic(topicName);
			if (topicObj == null) {
				throw new KafkaTopicNotFoundException(topicName);

			}
			builder.stream(topic).map((key, value) -> {
				process(topic, key, value, topicObj);
				return KeyValue.pair("key", "value");
			});
		}
	}

	@Override
	public JSONObject getSourceKafkaRecord(Topic topic, Object value) throws KStreamProcessingException {

		return applySchema(getSchema(topic), value);
	}

	private JSONObject getSchema(Topic topic) {
		return SchemaFactory.getInputKafkaTopicValueSchema(topic);
	}

	private JSONObject applySchema(JSONObject schema, Object value) throws KStreamProcessingException {
		return new SchemaUtils().applySchemaOverJSONString(schema, value.toString());
	}

	@Override
	public Producer<String, String> getProducer() {
		return producer;
	}

	public Properties configureSourceEndPointForStreaming()
			throws PipelineAuthenticationException, EndPointConfigurationNotFound {
	
			logger.info("Configuring streaming for Source Kafka");
			Properties kafkaSourceConfigs = new Properties();
			try {
				kafkaSourceConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG,
						AppStore.store.getPipelineConfiguration().get(PipelineConstants.PIPELINE_APP_NAME_MAPPING_KEY));
			} catch (Exception e) {
				throw new EndPointConfigurationNotFound(StreamsConfig.APPLICATION_ID_CONFIG);
			}

			try {
				kafkaSourceConfigs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, source.getBootstrapURL());

			} catch (Exception e) {
				throw new EndPointConfigurationNotFound(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
			}

			try {
				kafkaSourceConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppStore.store
						.getPipelineConfiguration().get(PipelineConstants.PIPELINE_CONSUMER_START_POINT_KEY));

			} catch (Exception e) {
				throw new EndPointConfigurationNotFound(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
			}

			kafkaSourceConfigs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			kafkaSourceConfigs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
					Serdes.String().getClass().getName());

			if (source.getIsAuthenticationEnabled()) {
				authenticateEndPointKafka(kafkaSourceConfigs, source);
			}
			return kafkaSourceConfigs;
	}

	@Override
	public void dispatchDestinationRecord(String key, String value, String destinationTopicName) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(destinationTopicName, key, value);

		producer.send(producerRecord, (metadata, exception) -> {
			if (exception != null) {
                     logger.error("Failed to send message to topic {} with key: {} and value: ", destinationTopicName, key,value);
                     System.exit(0);
			}
		});
	}
	
	@Override
	public JSONObject buildDestinationValue(JSONObject source, Topic topic) {
		return source;
	}

	private boolean checkConversionRequired(String sourceDataType, String destinationDataType) {
		return sourceDataType != null && destinationDataType != null && !sourceDataType.trim().isEmpty()
				&& !destinationDataType.trim().isEmpty();
	}

	@Override
	public Object applyDataTypeConversion(Object value, Field field) throws KStreamProcessingException {
		if (checkConversionRequired(field.getFieldSourceDataType(), field.getFieldDestinationDataType())) {
			try {
				return dataTypeConversionFactory
						.getConverter(field.getFieldSourceDataType(), field.getFieldDestinationDataType())
						.convert(value);
			} catch (DataConverterException e) {
				logger.error("Conversion failed for value: " + value + " from " + field.getFieldSourceDataType()
						+ " to " + field.getFieldDestinationDataType());
				// return value;
				e.handelDataConverterException();
			}
		} else {
			return value;
		}
		return null;
	}

	@Override
	public void implement() throws PipelineBuildException, PipelineAuthenticationException,
			EndPointConfigurationNotFound, KafkaTopicNotFoundException {
		logger.info("Implementing Pipeline");

		KafkaStreams streams = getKafkaStream(source.getUserDefinedTopicList());
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		
		logger.info("Successfully implemented pipeline");

	}

	public static DataTypeConversionFactory getDatatypeconversionfactory() {
		return dataTypeConversionFactory;
	}
}
