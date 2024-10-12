package com.lentra.realtime.authenticator.implementations;

import java.util.Map;
import java.util.Properties;

import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.exception.PipelineAuthenticationException;

public interface Authenticator {
    public abstract void addAuthenticationConfig(Properties endPointProperties, EndPointType endPointType) throws PipelineAuthenticationException;
    public void addSourceAuthenticationConfig(Properties kafkaSourceConfigs, Map<String, String> sourceEndPointConfigs) throws PipelineAuthenticationException;
    public void addDestinationAuthenticationConfig(Properties kafkaDestinationConfigs, Map<String, String> destinationEndPointConfigs) throws PipelineAuthenticationException;
}
