package com.lentra.realtime.utils;

import com.lentra.realtime.enums.TopicType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

public class TopicUtils 
{

	public List<String> getTopicNameFromUserDefinedTopicList(List<String> userDefinedTopicList) throws RuntimeException{

		return Collections.unmodifiableList(userDefinedTopicList.stream().flatMap(topics -> Stream.of(topics.split(",")))
				.map(topic -> topic.substring(topic.lastIndexOf('.') + 1)).filter(topic -> !topic.isEmpty()).collect(Collectors.toList()));
	}





}
