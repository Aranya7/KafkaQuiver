package com.lentra.realtime.entity.endpoint;

import com.lentra.realtime.authenticator.implementations.Authenticator;
import com.lentra.realtime.constants.EndPointConstants;
import com.lentra.realtime.constants.TopicConstants;
import com.lentra.realtime.entity.Field.Field;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.entity.Topic.TopicBuilder;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;
import com.lentra.realtime.exception.PipelineInitializationException;
import com.lentra.realtime.factories.AuthenticatorFactory;

import static com.lentra.realtime.constants.PipelineConstants.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.jetty.util.Fields;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SourceEndPoint extends AbstractEndPoint {
	private static final Logger logger = LogManager.getLogger(SourceEndPoint.class);

	public SourceEndPoint() {
		super(EndPointType.SOURCE);
	}

	@Override
	public String getBootStrapURL(Map<String, String> sourceConfigMap) {
		return sourceConfigMap.get(EndPointConstants.SOURCE_BOOTSTRAP_URL);
	}

	@Override
	public boolean checkAuthenticationEnabled(Map<String, String> sourceConfigMap) {
		return Boolean.parseBoolean(sourceConfigMap.get(EndPointConstants.SOURCE_AUTHENTICATION_ENABLED));
	}

	@Override
	public Authenticator getAuthenticator(Map<String, String> sourceConfigMap) {
		protocol = sourceConfigMap.get(EndPointConstants.SOURCE_SECURITY_PROTOCOL);
		typeOfAuthentication = sourceConfigMap.get(EndPointConstants.SOURCE_SASL_MECHANISM);
		return AuthenticatorFactory.getAuthenticator(isAuthenticationEnabled, protocol, typeOfAuthentication);
	}

	private void initializeBootstrapURL(Map<String, String> sourceConfigMap) throws PipelineInitializationException {
		this.bootstrapURL = getBootStrapURL(sourceConfigMap);
		if (bootstrapURL == null) {
			throw new PipelineInitializationException(EndPointConstants.SOURCE_BOOTSTRAP_URL);
		}
	}

	private void initializeIsAuthenticationEnabled(Map<String, String> sourceConfigMap){
		this.isAuthenticationEnabled = checkAuthenticationEnabled(sourceConfigMap);

	}

	private void initializeAuthenticator(Map<String, String> sourceConfigMap) throws PipelineInitializationException {
		this.authenticator = getAuthenticator(sourceConfigMap);
		if (authenticator == null) {
			throw new PipelineInitializationException(
					EndPointConstants.SOURCE_SECURITY_PROTOCOL + " or " + EndPointConstants.SOURCE_SASL_MECHANISM);
		}
	}

	@Override
	public void initializeEndPoint() throws PipelineInitializationException {
		Map<String, String> sourceConfigMap = getEndPointConfiguration();
		initializeBootstrapURL(sourceConfigMap);
		initializeIsAuthenticationEnabled(sourceConfigMap);
		initializeAuthenticator(sourceConfigMap);
		initializeEndPointTopics();
	}

	public void initializeTopicList(TopicBuilder builder) throws PipelineInitializationException {
		this.topicList = builder.getTopics();
		if (topicList == null || topicList.isEmpty()) {
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_LIST_KEY);
		}

	}

	public void initializeUserDefinedTopicList(TopicBuilder builder) throws PipelineInitializationException {
		this.userDefinedTopicList = builder.getUserDefinedSourceTopicList();
		if (userDefinedTopicList.isEmpty()) {
			throw new PipelineInitializationException(TopicConstants.SOURCE_TOPIC_LIST_KEY);
		}

	}

	@Override
	public boolean initializeEndPointTopics() throws PipelineInitializationException {
		logger.info("Building topics for endpoint.");
		TopicBuilder builder = new TopicBuilder(endPointType);
		initializeTopicList(builder);
		initializeUserDefinedTopicList(builder);

		return true;

	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Source BootStrapURL = " + bootstrapURL + "\n");
		sb.append("Source Authentication Enabled = " + isAuthenticationEnabled + "\n");
		sb.append("Source Authentication Type = " + typeOfAuthentication + "\n");
		sb.append("Source Authentication Protocol = " + protocol + "\n");
		sb.append("Source EndPoint Type = " + endPointType + "\n");
		sb.append("Source Extraction Topics = " + topicList + "\n");

		return sb.toString();
	}
}
