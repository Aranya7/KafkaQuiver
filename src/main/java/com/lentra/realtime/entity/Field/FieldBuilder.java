package com.lentra.realtime.entity.Field;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.EndPointConstants;
import com.lentra.realtime.constants.FieldConstants;
import com.lentra.realtime.constants.TopicConstants;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.exception.PipelineInitializationException;
import com.lentra.realtime.pipelines.impl.missioncontrol.MCKStreamImplementation;
import com.lentra.realtime.utils.FieldUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FieldBuilder
{
	private static Logger logger= LogManager.getLogger(MCKStreamImplementation.class);
	private Topic topic;
	private EndPointType endPointType;

	public FieldBuilder(Topic topic, EndPointType endPointType)
	{
		this.topic = topic;
		this.endPointType = endPointType;
	}

	private Set<String> getSourceTopicFieldSet() throws PipelineInitializationException
	{   String userDefinedString=AppStore.store.getSourceEndPointTopicConfiguration().get(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_LIST_SUFFIX);
		if(userDefinedString==null || userDefinedString.isEmpty()){
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_LIST_SUFFIX);
		}
		return new HashSet<>(new FieldUtils().getFieldListFromUserDefinedString(userDefinedString));
	}

	private Set<String> getDestinationTopicFieldSet() throws PipelineInitializationException
	{   String userDefinedString=AppStore.store.getDestinationEndPointConfiguration().get(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_LIST_SUFFIX);
		if(userDefinedString==null || userDefinedString.isEmpty()){
			throw new PipelineInitializationException(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_LIST_SUFFIX);
		}
		return new HashSet<>(new FieldUtils().getFieldListFromUserDefinedString(userDefinedString));
	}

	public Set<Field> getFields() throws PipelineInitializationException
	{
		return endPointType == EndPointType.SOURCE ? buildAllSourceTopicsFields() : buildAllDestinationTopicsFields();
	}

	//Future enhancements - Whitespace handling to be added
	private Set<Field> buildAllSourceTopicsFields() throws PipelineInitializationException
	{

		Map<String, String> topicSpecificMappings = AppStore.store.getSourceEndPointTopicConfiguration().entrySet().stream()
				.filter(entry -> entry.getKey().startsWith(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		if(topicSpecificMappings.isEmpty()){
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName());
		}
		Set<Field> topicFieldSet = new HashSet<>();

		Set<String> fieldNameSet = getSourceTopicFieldSet();
		if(fieldNameSet.isEmpty()){
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_LIST_SUFFIX);
		}
		fieldNameSet.stream().forEach(field ->{
			String jsonPath = topicSpecificMappings.get(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field);
			String pinotName = field;
			String sourceDataType = topicSpecificMappings.get(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field+ "." + FieldConstants.FIELD_MAPPING_SOURCE_DATATYPE);
			String destinationDataType = topicSpecificMappings.get(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field+ "." + FieldConstants.FIELD_MAPPING_DESTINATION_DATATYPE);
			String conditionalMapping = topicSpecificMappings.get(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field+ "." + FieldConstants.FIELD_MAPPING_ISCONDITIONAL);
			boolean isConditional = Boolean.parseBoolean(conditionalMapping!=null ? conditionalMapping : "false");
			Field newField = null;
			try {
				newField = new Field(jsonPath, pinotName, sourceDataType, destinationDataType, isConditional);
			} catch (PipelineInitializationException e) {
				logger.error("Error occured in reading property : "+TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field);
				e.handleFieldException();
			}
			topicFieldSet.add(newField);
		});
		return topicFieldSet;

	}

	//Future enhancements - Whitespace handling to be added
	private Set<Field> buildAllDestinationTopicsFields() throws PipelineInitializationException
	{
		Map<String, String> topicSpecificMappings = AppStore.store.getDestinationEndPointTopicConfiguration().entrySet().stream()
				.filter(entry -> entry.getKey().startsWith(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		Set<Field> topicFieldSet = new HashSet<>();

		Set<String> fieldNameList = getDestinationTopicFieldSet();
		if(fieldNameList.isEmpty()){
			throw new PipelineInitializationException(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_LIST_SUFFIX);
		}
		fieldNameList.stream().forEach(field ->{
			String jsonPath = topicSpecificMappings.get(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field);
			String pinotName = field;
			String sourceDataType = topicSpecificMappings.get(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field+ "." + FieldConstants.FIELD_MAPPING_SOURCE_DATATYPE);
			String destinationDataType = topicSpecificMappings.get(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field+ "." + FieldConstants.FIELD_MAPPING_DESTINATION_DATATYPE);
			String conditionalMapping = topicSpecificMappings.get(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field+ "." + FieldConstants.FIELD_MAPPING_ISCONDITIONAL);
			boolean isConditional = Boolean.parseBoolean(conditionalMapping!=null ? conditionalMapping : "false");
			Field newField = null;
			try {
				newField = new Field(jsonPath, pinotName, sourceDataType, destinationDataType, isConditional);
			} catch (PipelineInitializationException e) {
				logger.error("Error occured in reading property : "+TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_FIELD_SUFFIX + field);
				e.handleFieldException();
			}
			topicFieldSet.add(newField);
		});
		return topicFieldSet;


	}
}