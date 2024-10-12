package com.lentra.realtime.schema;

import com.lentra.realtime.exception.KStreamProcessingException;
import com.lentra.realtime.exception.SchemeBuilderException;
import org.json.JSONObject;

public interface SchemaBuilder 
{
	public void buildSchema() throws SchemeBuilderException;
	public JSONObject getSchemaJSONObject();
}
