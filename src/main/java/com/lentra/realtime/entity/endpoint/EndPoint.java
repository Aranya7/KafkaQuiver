package com.lentra.realtime.entity.endpoint;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.lentra.realtime.authenticator.implementations.Authenticator;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.exception.PipelineBuildException;
import com.lentra.realtime.exception.PipelineInitializationException;

public interface EndPoint
{
    public void setBootstrapURL(String bootstrapURL);
    public void setIsAuthenticationEnabled(boolean isAuthenticationEnabled);
    public void setTypeOfAuthentication(String typeOfAuthentication);
    public void setProtocol(String protocol);
    public void setTopicList(Set<Topic> topicList);
    public void initializeEndPoint() throws PipelineInitializationException;
    public Map<String,String> getEndPointConfiguration();
    public boolean initializeEndPointTopics() throws PipelineInitializationException;
    
    public String getBootstrapURL();
    public boolean getIsAuthenticationEnabled();
    public Authenticator getAuthenticator();
    
    public String getBootStrapURL(Map<String,String> sourceConfigMap);
    public boolean checkAuthenticationEnabled(Map<String,String> sourceConfigMap);
    public Authenticator getAuthenticator(Map<String,String> sourceConfigMap);


    public Set<Topic> getTopicList();
    public List<String> getUserDefinedTopicList();
    public Topic getTopic(String topicName) throws PipelineBuildException;
    public EndPointType getEndPointType();
}
