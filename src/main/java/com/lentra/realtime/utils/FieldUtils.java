package com.lentra.realtime.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.lentra.realtime.constants.TopicConstants;
import com.lentra.realtime.entity.Field.Field;
import com.lentra.realtime.exception.PipelineInitializationException;

public class FieldUtils {

	public List<String> getFieldListFromUserDefinedString(String userDefinedString) throws PipelineInitializationException
	{
		return new ArrayList<>(Arrays.asList(userDefinedString.split(",")));
    }

	
}
