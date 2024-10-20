package com.lentra.realtime.pipelines;

import com.lentra.realtime.entity.endpoint.EndPoint;
import com.lentra.realtime.enums.OffsetPositions;
import com.lentra.realtime.exception.*;

import java.io.IOException;

public interface Pipeline
{
	public void initialize() throws PipelineInitializationException;
	public void initializeSource() throws PipelineInitializationException;
	public void initializeDestination() throws PipelineInitializationException;
	public void start() throws PipelineBuildException, PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException, PropertyObservableUpdatedFileLoadException, PropertyObservableFileComparisonException;
	public void stop();

	
	public String getPipelineAppName();

	public void setPipelineAppName(String pipelineAppName);
	
	public OffsetPositions getReadingOffsetPosition();

	public void setReadingOffsetPosition(OffsetPositions readingOffsetPosition);
	public EndPoint getSourceEndPoint();
}
