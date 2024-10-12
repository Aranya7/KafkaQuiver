package com.lentra.realtime.factories;

import com.lentra.realtime.exception.SchemeBuilderException;
import org.json.JSONObject;

import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.enums.TopicType;
import com.lentra.realtime.schema.CDCSchemaBuilder;
import com.lentra.realtime.schema.DomainEventSchemaBuilder;
import com.lentra.realtime.schema.SchemaBuilder;

public final class SchemaFactory 
{	
	private static final SchemaBuilder domainEventSchemaBuilder = new DomainEventSchemaBuilder();
	private static final SchemaBuilder cdcSchemaBuilder;

    static {
        try {
            cdcSchemaBuilder = new CDCSchemaBuilder();
        } catch (SchemeBuilderException e) {
            throw new RuntimeException(e);
        }
    }

    public static final JSONObject getInputKafkaTopicValueSchema(Topic topic)
	{
		return topic.getTopicType() == TopicType.CDC ? getCDCSchema(): getDESchema();
	}

	private static final JSONObject getDESchema() {
		return domainEventSchemaBuilder.getSchemaJSONObject();
	}

	private static final JSONObject getCDCSchema() {
		return cdcSchemaBuilder.getSchemaJSONObject();
	}
}
