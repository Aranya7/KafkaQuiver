package com.lentra.realtime.mappers;

import com.lentra.realtime.exception.PipelineConfiguationContentNotFoundException;
import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public interface MapperReader
{
	public abstract Optional<Map<String,String>> getPipelineConfiguration() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException;
}
