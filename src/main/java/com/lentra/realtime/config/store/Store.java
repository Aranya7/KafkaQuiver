package com.lentra.realtime.config.store;

import java.util.Map;

public interface Store 
{
	public Map<String,String> buildPipelineConfiguration();
	public Map<String,String> buildSourceEndPointConfiguration();
	public Map<String,String> buildDestinationEndPointConfiguration();
	public Map<String,String> buildSourceTopicConfiguration();
	public Map<String,String> buildDestinationTopicConfiguration();
	public Map<String,String> buildUserMapperConfiguration();
	
	public Map<String,String> getPipelineConfiguration();
	public Map<String,String> getSourceEndPointConfiguration();
	public Map<String,String> getSourceEndPointTopicConfiguration();
	public Map<String,String> getDestinationEndPointConfiguration();
	public Map<String,String> getDestinationEndPointTopicConfiguration();
	public String getPropertiesFileFormat();
}
