package com.lentra.realtime.entity.endpoint;

import com.lentra.realtime.authenticator.implementations.Authenticator;
import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.exception.PipelineBuildException;
import com.lentra.realtime.exception.PipelineInitializationException;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractEndPoint implements EndPoint
{
	protected String bootstrapURL;
	protected boolean isAuthenticationEnabled;
	protected String typeOfAuthentication; // We will convert into Enum
	protected String protocol; // We will convert into Enum 
	protected Set<Topic> topicList;
	protected List<String> userDefinedTopicList;
	protected EndPointType endPointType;
	protected Authenticator authenticator;
	
	protected AbstractEndPoint(EndPointType endPointType){
		this.endPointType=endPointType;
	}

	public List<String> getUserDefinedTopicList() {
		return userDefinedTopicList;
	}

	@Override
	public void setBootstrapURL(String bootstrapURL) {
		this.bootstrapURL=bootstrapURL;
	}

	@Override
	public void setIsAuthenticationEnabled(boolean isAuthenticationEnabled) {
		this.isAuthenticationEnabled=isAuthenticationEnabled;
	}

	@Override
	public void setTypeOfAuthentication(String typeOfAuthentication) {
		this.typeOfAuthentication=typeOfAuthentication;
	}

	@Override
	public void setProtocol(String protocol) {
		this.protocol=protocol;
	}

	@Override
	public void setTopicList(Set<Topic> topicList) {
		this.topicList=topicList;
	}
	
	@Override
	public String getBootstrapURL() {
		return this.bootstrapURL;
	}
	
	@Override
	public boolean getIsAuthenticationEnabled() {
		return this.isAuthenticationEnabled;
	}
	
	@Override
	public Authenticator getAuthenticator() {
		return authenticator;
	}

	public void setAuthenticator(Authenticator authenticator) {
		this.authenticator = authenticator;
	}

	public Set<Topic> getTopicList() {
		return topicList;
	}
	
	@Override
	public Topic getTopic(String topicName) throws PipelineBuildException {
		List<Topic> filteredTopics = getTopicList().stream()
				.filter(topic -> topic.getTopicName().equalsIgnoreCase(topicName))
				.collect(Collectors.toList());
		return  filteredTopics.isEmpty() ? null : filteredTopics.get(0);
    }

	@Override
	public Map<String,String> getEndPointConfiguration() {
		       
		return this.endPointType.equals(EndPointType.SOURCE) ? AppStore.store.getSourceEndPointConfiguration() 
				: AppStore.store.getDestinationEndPointConfiguration();
	}

	public EndPointType getEndPointType() {
		return endPointType;
	}

	public void setEndPointType(EndPointType endPointType) {
		this.endPointType = endPointType;
	}
}
