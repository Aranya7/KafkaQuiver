package com.lentra.realtime.entity.Field;

import com.lentra.realtime.exception.PipelineInitializationException;

public class Field
{
	private String fieldSourceJsonPath;
	private String piontFieldName;
	private String fieldSourceDataType; // We will convert into enum
	private String fieldDestinationDataType; // We will convert into enum
	private boolean isConditional;

	public Field(String fieldSourceJsonPath, String pinotFieldName, String fieldSourceDataType, String destinationDataType, boolean isConditional) throws PipelineInitializationException {
		this.fieldSourceJsonPath = fieldSourceJsonPath;
		this.piontFieldName = pinotFieldName;
		this.fieldSourceDataType = fieldSourceDataType;
		this.fieldDestinationDataType = destinationDataType;
		this.isConditional=isConditional;

		if(fieldSourceJsonPath==null || fieldSourceJsonPath.isEmpty()){
			throw new PipelineInitializationException();
		}
	}

	public String getFieldSourceJsonPath() {
		return fieldSourceJsonPath;
	}

	public boolean isConditional() {
		return isConditional;
	}

	public void setConditional(boolean conditional) {
		isConditional = conditional;
	}

	public void setFieldSourceJsonPath(String fieldSourceJsonPath) {
		this.fieldSourceJsonPath = fieldSourceJsonPath;
	}



	public String getPiontFieldName() {
		return piontFieldName;
	}



	public void setPiontFieldName(String piontFieldName) {
		this.piontFieldName = piontFieldName;
	}



	public String getFieldSourceDataType() {
		return fieldSourceDataType;
	}



	public void setFieldSourceDataType(String fieldSourceDataType) {
		this.fieldSourceDataType = fieldSourceDataType;
	}



	public String getFieldDestinationDataType() {
		return fieldDestinationDataType;
	}



	public void setFieldDestinationDataType(String fieldDestinationDataType) {
		this.fieldDestinationDataType = fieldDestinationDataType;
	}



	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public String toString() {
		StringBuilder fieldString = new StringBuilder();
		fieldString.append("Field Source JSON Path = " + fieldSourceJsonPath + "\n");
		fieldString.append("Field Pinot Name = " + piontFieldName + "\n");
		fieldString.append("Field Source DataType = " + fieldSourceDataType + "\n");
		fieldString.append("Field Destination DataType = " + fieldDestinationDataType + "\n");
		fieldString.append("Field Conditional = " + isConditional + "\n");
		return fieldString.toString();
	}
}