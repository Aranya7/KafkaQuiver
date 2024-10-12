package com.lentra.realtime.entity.Topic;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.lentra.realtime.constants.TopicConstants;
import com.lentra.realtime.entity.Field.Field;
import com.lentra.realtime.enums.TopicType;
import com.lentra.realtime.exception.PipelineInitializationException;

public class Topic 
{
	private Set<Field> fieldMappings= new LinkedHashSet<>();;
	private TopicType topicType; // We will convert into Enum (CDC, DE)
	private Set<String> topicKeyFields;
	private String topicName;
	private Topic destinationTopic;

	public Topic(){

	}

	public Topic(String topicName){
		this.topicName=topicName;
		this.destinationTopic=new Topic();
	}
	public Set<Field> getFieldMappings() {
		return fieldMappings;
	}

	public void setFieldMappings(Set<Field> fieldMappings) {
		this.fieldMappings = fieldMappings;
	}

	public Topic getDestinationTopic() {
		return destinationTopic;
	}

	public void setDestinationTopic(Topic destinationTopic) {
		this.destinationTopic = destinationTopic;
	}

	public TopicType getTopicType() {
		return topicType;
	}

	public void setTopicType(TopicType topicType) throws PipelineInitializationException {
		this.topicType = topicType;
	}

	public Set<String> getTopicKeyFields() {
		return topicKeyFields;
	}

	public void setTopicKeyFields(Set<String> topicKeyFields) {
		this.topicKeyFields = topicKeyFields;
	}

	public void setTopicKeyFields(List<String> topicKeyFields) throws PipelineInitializationException {
		if(topicKeyFields.get(0)==null|| topicKeyFields.get(0).isEmpty()){
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + getTopicName() + "." + TopicConstants.TOPIC_MAPPING_KEY_FIELD);

		}
		Set<String> keyFieldSet = new HashSet<>(topicKeyFields);
		this.topicKeyFields = keyFieldSet;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	@Override
	public int hashCode() {
		return super.hashCode();
	}
	
	@Override
	public String toString() {
		StringBuilder topicString = new StringBuilder();
		topicString.append("Topic Name = " + topicName + "\n");
		topicString.append("Topic Key Fields List = " + topicKeyFields + "\n");
		topicString.append("Topic Destination Topics = " + destinationTopic.getTopicName() + "\n");
		topicString.append("Topic Type = " + topicType + "\n");
		topicString.append("Topic Mapping Fields = " + fieldMappings + "\n");
		return topicString.toString();
	}
}
