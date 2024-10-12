package com.lentra.realtime.authenticator.implementations;

import java.util.Map;
import java.util.Properties;

import com.lentra.realtime.enums.EndPointType;

public class DefaultAuthenticator implements Authenticator 
{

	@Override
	public void addAuthenticationConfig(Properties endPointProperties, EndPointType endPointType) {

	}

	@Override
	public void addSourceAuthenticationConfig(Properties kafkaSourceConfigs, Map<String, String> sourceEndPointConfigs) {
		
	}

	@Override
	public void addDestinationAuthenticationConfig(Properties kafkaDestinationConfigs, Map<String, String> destinationEndPointConfigs) {

	}
}
