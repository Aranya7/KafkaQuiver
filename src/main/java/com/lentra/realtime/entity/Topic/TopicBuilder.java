package com.lentra.realtime.entity.Topic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.lentra.realtime.MCKStreamRealtime.App;
import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.PipelineConstants;
import com.lentra.realtime.constants.TopicConstants;
import com.lentra.realtime.entity.Field.FieldBuilder;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.enums.TopicType;
import com.lentra.realtime.exception.PipelineInitializationException;
import com.lentra.realtime.utils.TopicUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TopicBuilder {
	private static Logger logger = LogManager.getLogger(TopicBuilder.class);

	private EndPointType endPointType;

	public TopicBuilder(EndPointType endPointType) {
		this.endPointType = endPointType;
	}

	public List<String> getUserDefinedSourceTopicList() throws PipelineInitializationException {
		return Collections
				.unmodifiableList(new ArrayList<>(Arrays.asList(AppStore.store.getSourceEndPointTopicConfiguration()
						.get(TopicConstants.SOURCE_TOPIC_LIST_KEY).split(TopicConstants.SOURCE_TOPIC_LIST_SEPARATOR))));

	}

	public List<String> getUserDefinedDestinationTopicList() {
		return Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
				AppStore.store.getDestinationEndPointTopicConfiguration().get(TopicConstants.DESTINATION_TOPIC_LIST_KEY)
						.split(TopicConstants.DESTINATION_TOPIC_LIST_SEPARATOR))));
	}

	public List<String> getSourceTopicNamesFromUserDefinedList() throws PipelineInitializationException {
		return new TopicUtils().getTopicNameFromUserDefinedTopicList(getUserDefinedSourceTopicList());
	}

	public List<String> getDestinationTopicNamesFromUserDefinedList() throws RuntimeException {
		return new TopicUtils().getTopicNameFromUserDefinedTopicList(getUserDefinedDestinationTopicList());
	}

	public Set<Topic> getTopics() throws PipelineInitializationException {
		return endPointType == EndPointType.SOURCE ? buildAllSourceTopics() : buildAllDestinationTopics();
	}

	private Set<Topic> buildAllSourceTopics() throws PipelineInitializationException {
		List<String> sourceTopicList = getSourceTopicNamesFromUserDefinedList();
		if (sourceTopicList.isEmpty()) {
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_LIST_KEY);
		}

		Set<Topic> topicSet = new HashSet<>();
		for (String topicName : sourceTopicList) {
			Topic topic = new Topic(topicName);
			topicSet.add(topic);
			buildTopicAttributes(topic);
			buildTopicMappings(topic);
		}

		return topicSet;

	}

	private Set<Topic> buildAllDestinationTopics() throws PipelineInitializationException {

		List<String> destinationTopicList = getDestinationTopicNamesFromUserDefinedList();
		if (destinationTopicList.get(0).isEmpty()) {

			throw new PipelineInitializationException(TopicConstants.DESTINATION_TOPIC_LIST_KEY);
		}
		Set<Topic> topicSet = new HashSet<>();
		for (String topicName : destinationTopicList) {
			Topic topic = new Topic(topicName);
			topicSet.add(topic);
			buildTopicAttributes(topic);
			buildTopicMappings(topic);
		}
		return topicSet;
	}

	private void buildTopicAttributes(Topic topic) throws PipelineInitializationException {
		if (endPointType == EndPointType.SOURCE)
			buildSourceTopicAttributes(topic);
		else
			buildDestinationTopicAttributes(topic);
	}

	private void buildSourceTopicAttributes(Topic topic) throws PipelineInitializationException {
		Map<String, String> topicSpecificMapping = getTopicMappings(topic.getTopicName());
		if (topicSpecificMapping == null || topicSpecificMapping.isEmpty()) {
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName());
		}
		String eventType = topicSpecificMapping.get(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "."
				+ TopicConstants.TOPIC_MAPPING_EVENT_TYPE);
		if (eventType == null || eventType.isEmpty()) {
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "."
					+ TopicConstants.TOPIC_MAPPING_EVENT_TYPE);
		}
		if (!Arrays.stream(TopicType.values()).anyMatch(type -> type.name().equalsIgnoreCase(eventType))) {
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "."
					+ TopicConstants.TOPIC_MAPPING_EVENT_TYPE);

		}
		topic.setTopicType(TopicType.valueOf(eventType));
		String keyFields = topicSpecificMapping.get(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "."
				+ TopicConstants.TOPIC_MAPPING_KEY_FIELD);
		if (keyFields == null || keyFields.isEmpty()) {
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "."
					+ TopicConstants.TOPIC_MAPPING_KEY_FIELD);
		}
		topic.setTopicKeyFields(Arrays.asList(keyFields));
		String destinationTopic = topicSpecificMapping.get(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName()
				+ "." + TopicConstants.TOPIC_MAPPING_DESTINATION_TOPIC);
		if (destinationTopic == null || destinationTopic.isEmpty()) {
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_PREFIX + topic.getTopicName() + "."
					+ TopicConstants.TOPIC_MAPPING_DESTINATION_TOPIC);
		}
		topic.getDestinationTopic().setTopicName(destinationTopic);

	}

	private void buildDestinationTopicAttributes(Topic topic) throws PipelineInitializationException {
		Map<String, String> topicSpecificMapping = getTopicMappings(topic.getTopicName());
		if (topicSpecificMapping == null || topicSpecificMapping.isEmpty()) {
			throw new PipelineInitializationException(TopicConstants.DESTINATION_TOPIC_PREFIX + topic.getTopicName());
		}
		topic.setTopicType(TopicType.valueOf(topicSpecificMapping.get(TopicConstants.DESTINATION_TOPIC_PREFIX
				+ topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_EVENT_TYPE)));
		topic.setTopicKeyFields(Arrays.asList(topicSpecificMapping.get(TopicConstants.DESTINATION_TOPIC_PREFIX
				+ topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_KEY_FIELD)));
		topic.getDestinationTopic().setTopicName(topicSpecificMapping.get(TopicConstants.DESTINATION_TOPIC_PREFIX
				+ topic.getTopicName() + "." + TopicConstants.TOPIC_MAPPING_DESTINATION_TOPIC));

	}

	private Map<String, String> getTopicMappings(String topicName) throws PipelineInitializationException {
		return endPointType == EndPointType.SOURCE
				? AppStore.store.getSourceEndPointTopicConfiguration().entrySet().stream()
						.filter(x -> x.getKey().startsWith(TopicConstants.SOURCE_TOPIC_PREFIX + topicName))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
				: AppStore.store.getDestinationEndPointTopicConfiguration().entrySet().stream()
						.filter(x -> x.getKey().startsWith(TopicConstants.DESTINATION_TOPIC_PREFIX + topicName))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	private void buildTopicMappings(Topic topic) throws PipelineInitializationException {
		FieldBuilder builder = new FieldBuilder(topic, endPointType);
		topic.setFieldMappings(builder.getFields());
	}
}
