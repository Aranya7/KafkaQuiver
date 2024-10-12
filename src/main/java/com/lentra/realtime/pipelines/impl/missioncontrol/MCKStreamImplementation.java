package com.lentra.realtime.pipelines.impl.missioncontrol;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.jayway.jsonpath.PathNotFoundException;
import com.lentra.realtime.enums.TopicType;
import com.lentra.realtime.exception.GetDestinationValueException;
import com.lentra.realtime.exception.KStreamProcessingException;
import com.lentra.realtime.exception.PipelineImplementationException;
import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;

import com.jayway.jsonpath.JsonPath;
import com.lentra.realtime.entity.Field.Field;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.pipelines.impl.AbstractImplementation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MCKStreamImplementation extends AbstractImplementation
{	
	private static Logger logger=LogManager.getLogger(MCKStreamImplementation.class);

	@Override
	public KeyValue<Object, Object> process(String topicName, Object key, Object value, Topic topicObj) {
		JSONObject sourceKafkaRecord = null;

			try{
				sourceKafkaRecord = getSourceKafkaRecord(topicObj, value);

			} catch (KStreamProcessingException e) {
				logger.info("Error In Processing sourceKafkaRecord : {}",sourceKafkaRecord,e);
				System.exit(0);
			}

			JSONObject destinationValue = buildDestinationValue(sourceKafkaRecord, topicObj);
			String destinationKey = buildDestinationKey(destinationValue, topicObj.getTopicKeyFields());
			dispatchDestinationRecord(destinationKey, destinationValue.toString(), topicObj.getDestinationTopic().getTopicName());
			return KeyValue.pair(destinationKey, destinationValue.toString());

	/*	} catch (Exception e) {
			logger.info("MESSAGE REJECTED - Record: {} \nRejection Reason: {}",sourceKafkaRecord,e);
			System.exit(0);
			return null;
		} */

	}

	@Override
	public final JSONObject buildDestinationValue(JSONObject kafkaValueRecord, Topic topic) 
	{
		try{
			JSONObject destinationValue = new JSONObject();
			topic.getFieldMappings().stream().forEach(field -> {
				processFieldMapping(field, kafkaValueRecord, destinationValue);
			});
			return destinationValue;
		} catch (Exception e) {
			logger.info("MESSAGE REJECTED while building destination value - Record: {} \nRejection Reason: {}",kafkaValueRecord,e);
			return null;

		}

	}
	
	private final Object getJsonValueByPath(JSONObject json, String path) throws GetDestinationValueException {
		if(json==null){
			throw new GetDestinationValueException(json);
		}
		Object value;
		try{
			value = JsonPath.read(json.toString(), path);
		}

		catch (PathNotFoundException e){
			logger.debug("Error when extracting {} path from the record \n Exception occurred: {}",path,e);
			value = null;
		}
		return value;
    }
	
	private void processFieldMapping(Field field, JSONObject kafkaValueRecord, JSONObject destinationValue) {
        Object value = null;
		try{
			value = getJsonValueByPath(kafkaValueRecord, field.getFieldSourceJsonPath());
		} catch (GetDestinationValueException e) {
           e.handleException();
        }
        try {
			if (value != null) {
				value = handleConditionalField(value, field);
				value = handleDiscrepancy(value, field);
				value = applyDataTypeConversion(value, field);
			}
		}catch (Exception e) {
        	logger.error("Data Processing Error: {}",e);
            value = null;
        }
        
        destinationValue.put(field.getPiontFieldName(), value);
    }

	private Object handleDiscrepancy(Object value, Field field)
	{

		if(value!=null && value.toString().equals("NaN")) {
			value = "0";
		}

		//Handling application_id values containing "$oid"
		//need to check below condition
		if(field.getFieldSourceJsonPath().equals("$.after._id") && value.toString().contains("$oid")){
			value= null;
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	private Object handleConditionalField(Object value, Field field) throws GetDestinationValueException {
		if(!(Arrays.asList(value).size()==1)){
			throw  new GetDestinationValueException(String.valueOf(value));
		}
		try{
			return field.isConditional() ? ((List<Object>) value).get(0) : value;
			//check size of array otherwise throw

		} catch (IndexOutOfBoundsException e){
			throw  new GetDestinationValueException(String.valueOf(e));
		}

	}

	@Override
	public final String buildDestinationKey(JSONObject destinationRecord, Set<String> keyField) {
		String[] keyFieldValues = new String[keyField.toArray().length];
		int i = 0;
        for (String key : keyField) {
            keyFieldValues[i] = String.valueOf(destinationRecord.get(key));
            i += 1;
        }
        return String.join("_", keyFieldValues);
	}
	
	
}
