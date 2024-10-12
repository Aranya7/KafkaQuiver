package com.lentra.realtime.schema;

import com.lentra.realtime.exception.KStreamProcessingException;
import com.lentra.realtime.exception.PipelineImplementationException;
import com.lentra.realtime.exception.SchemeBuilderException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public final class CDCSchemaBuilder implements SchemaBuilder{
	private JSONObject schemaJSONObject;
	private static final Logger logger= LogManager.getLogger(CDCSchemaBuilder.class);

	public CDCSchemaBuilder() throws SchemeBuilderException {
		buildSchema();
	}

	@Override
	public void buildSchema()  {
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream schemaStream = classLoader.getResourceAsStream("CDCSchema.json");


		if (schemaStream != null) {
			//InputStream schemaStream1=null;
			schemaJSONObject = new JSONObject(convertStreamToString(schemaStream));
		} else {

			//throw
			logger.info("CDCSchema.json not found in resources.");
		//	throw new SchemeBuilderException("CDCSchema.json");


			//System.err.println();
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
