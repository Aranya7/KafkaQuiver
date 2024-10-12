package com.lentra.realtime.schema;

import com.lentra.realtime.exception.PipelineImplementationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public final class DomainEventSchemaBuilder implements SchemaBuilder 
{
	private static final  Logger logger= LogManager.getLogger(DomainEventSchemaBuilder.class);
	private JSONObject schemaJSONObject;

	public DomainEventSchemaBuilder() 
	{
		buildSchema();
	}

	@Override
	public void buildSchema() {
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream schemaStream = classLoader.getResourceAsStream("DomainEventSchema.json");


		if (schemaStream != null) {
			schemaJSONObject = new JSONObject(convertStreamToString(schemaStream));
		} else {
			logger.info("DomainEventSchema.json not found in resources.");
		}
	}

	private String convertStreamToString(InputStream is) {
		Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name());
		String text = scanner.useDelimiter("\\A").next();
		scanner.close();
		return text;
	}

	@Override
	public JSONObject getSchemaJSONObject() {
		return schemaJSONObject;
	}
}
