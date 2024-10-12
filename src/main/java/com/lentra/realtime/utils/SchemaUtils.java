package com.lentra.realtime.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lentra.realtime.exception.KStreamProcessingException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaUtils {

	private static final Logger logger = LogManager.getLogger(SchemaUtils.class);

	/**
	 * Applies a schema over a JSON string and processes special keys like "after".
	 *
	 * @param schema           The schema to apply.
	 * @param jsonObjectString The JSON string to apply the schema on.
	 * @return The resulting JSONObject after schema application.
	 * @throws KStreamProcessingException if there is a processing error.
	 */
	public JSONObject applySchemaOverJSONString(JSONObject schema, String jsonObjectString) throws KStreamProcessingException {
		JSONObject jsonValue;
		JSONObject result = new JSONObject();

		// Parse the input JSON string into a JSONObject
		try {
			jsonValue = new JSONObject(new JSONTokener(jsonObjectString));
		} catch (JSONException e) {
			logger.info("Exception occurred while applying schema: {}", e);
			throw new KStreamProcessingException(e.toString());
		}

		// Iterate over schema keys to process and apply them to the result
		for (String key : schema.keySet()) {
			if (jsonValue.has(key)) {
				if (key.equalsIgnoreCase("after")) {
					String after = jsonValue.getString(key);
					ObjectMapper objectMapper = new ObjectMapper();
					objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);

					// Parse the 'after' JSON string
					try {
						JSONObject afterJsonObject = new JSONObject(objectMapper.readTree(after).toString());
						result.put(key, afterJsonObject);
					} catch (JsonProcessingException e) {
						throw new KStreamProcessingException(after);
					}
				} else {
					result.put(key, jsonValue.get(key));
				}
			}
		}

		return result;
	}
}
