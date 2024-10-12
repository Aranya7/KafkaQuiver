package com.lentra.realtime.pipelines.impl;

import java.util.List;
import java.util.Set;

import com.lentra.realtime.exception.*;
import com.lentra.realtime.pipelines.Pipeline;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;

import com.lentra.realtime.entity.Field.Field;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.entity.endpoint.EndPoint;

public interface PipelineImplementation 
{
	public void implement() throws PipelineBuildException, PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException;
	public void initialize(EndPoint source, EndPoint target, Pipeline pipeline) throws EndPointConfigurationNotFound, PipelineAuthenticationException;
	public KafkaStreams getKafkaStream(List<String> topicList) throws PipelineBuildException, PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException;
	public JSONObject getSourceKafkaRecord(Topic topic, Object value) throws KStreamProcessingException;
	public String buildDestinationKey(JSONObject source, Set<String> keyField);
	public JSONObject buildDestinationValue(JSONObject source, Topic topic);
	public Producer<String, String> getProducer();
	public void dispatchDestinationRecord(String key, String value, String destinationTopicName) throws KStreamProcessingException;
	public KeyValue<Object, Object> process(String topicName, Object key, Object value, Topic topicObj);
	public Object applyDataTypeConversion(Object value, Field field) throws KStreamProcessingException;
}
