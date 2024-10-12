package com.lentra.realtime.entity.Field;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.lentra.realtime.exception.PipelineInitializationException;

public class FieldTest {
    private Field field;
    private static final String SOURCE_JSON_PATH = "$.data.field";
    private static final String PINOT_FIELD_NAME = "test_field";
    private static final String SOURCE_DATA_TYPE = "STRING";
    private static final String DESTINATION_DATA_TYPE = "VARCHAR";
    private static final boolean IS_CONDITIONAL = true;

    @BeforeEach
    public void setUp() throws PipelineInitializationException {
        field = new Field(SOURCE_JSON_PATH, PINOT_FIELD_NAME, SOURCE_DATA_TYPE,
                DESTINATION_DATA_TYPE, IS_CONDITIONAL);
    }

    @Test
    public void testValidConstructor() throws PipelineInitializationException {
        Field testField = new Field(SOURCE_JSON_PATH, PINOT_FIELD_NAME, SOURCE_DATA_TYPE,
                DESTINATION_DATA_TYPE, IS_CONDITIONAL);

        assertNotNull(testField, "Field object should not be null");
        assertEquals(SOURCE_JSON_PATH, testField.getFieldSourceJsonPath(), "Field source JSON path should match");
        assertEquals(PINOT_FIELD_NAME, testField.getPiontFieldName(), "Pinot field name should match");
        assertEquals(SOURCE_DATA_TYPE, testField.getFieldSourceDataType(), "Source data type should match");
        assertEquals(DESTINATION_DATA_TYPE, testField.getFieldDestinationDataType(), "Destination data type should match");
        assertTrue(testField.isConditional(), "isConditional should be true");
    }

    @Test
    public void testConstructorWithNullJsonPath() {
        assertThrows(PipelineInitializationException.class, () -> {
            new Field(null, PINOT_FIELD_NAME, SOURCE_DATA_TYPE, DESTINATION_DATA_TYPE, IS_CONDITIONAL);
        });
    }

    @Test
    public void testConstructorWithEmptyJsonPath() {
        assertThrows(PipelineInitializationException.class, () -> {
            new Field("", PINOT_FIELD_NAME, SOURCE_DATA_TYPE, DESTINATION_DATA_TYPE, IS_CONDITIONAL);
        });
    }

    @Test
    public void testConstructorWithNullOptionalFields() throws PipelineInitializationException {
        Field testField = new Field(SOURCE_JSON_PATH, null, null, null, IS_CONDITIONAL);

        assertEquals(SOURCE_JSON_PATH, testField.getFieldSourceJsonPath(), "Field source JSON path should match");
        assertNull(testField.getPiontFieldName(), "Pinot field name should be null");
        assertNull(testField.getFieldSourceDataType(), "Source data type should be null");
        assertNull(testField.getFieldDestinationDataType(), "Destination data type should be null");
    }

    @Test
    public void testSetAndGetFieldSourceJsonPath() {
        String newPath = "$.data.newField";
        field.setFieldSourceJsonPath(newPath);
        assertEquals(newPath, field.getFieldSourceJsonPath(), "Field source JSON path should be updated");
    }

    @Test
    public void testSetAndGetPinotFieldName() {
        String newFieldName = "new_test_field";
        field.setPiontFieldName(newFieldName);
        assertEquals(newFieldName, field.getPiontFieldName(), "Pinot field name should be updated");
    }

    @Test
    public void testSetAndGetFieldSourceDataType() {
        String newDataType = "INTEGER";
        field.setFieldSourceDataType(newDataType);
        assertEquals(newDataType, field.getFieldSourceDataType(), "Source data type should be updated");
    }

    @Test
    public void testSetAndGetFieldDestinationDataType() {
        String newDataType = "INT";
        field.setFieldDestinationDataType(newDataType);
        assertEquals(newDataType, field.getFieldDestinationDataType(), "Destination data type should be updated");
    }

    @Test
    public void testSetAndGetConditional() {
        field.setConditional(false);
        assertFalse(field.isConditional(), "isConditional should be false");

        field.setConditional(true);
        assertTrue(field.isConditional(), "isConditional should be true");
    }

    @Test
    public void testToString() {
        String expectedString = new StringBuilder()
                .append("Field Source JSON Path = " + SOURCE_JSON_PATH + "\n")
                .append("Field Pinot Name = " + PINOT_FIELD_NAME + "\n")
                .append("Field Source DataType = " + SOURCE_DATA_TYPE + "\n")
                .append("Field Destination DataType = " + DESTINATION_DATA_TYPE + "\n")
                .append("Field Conditional = " + IS_CONDITIONAL + "\n")
                .toString();

        assertEquals(expectedString, field.toString(), "toString output should match expected format");
    }

    @Test
    public void testToStringWithNullFields() throws PipelineInitializationException {
        Field nullField = new Field(SOURCE_JSON_PATH, null, null, null, false);
        String expectedString = new StringBuilder()
                .append("Field Source JSON Path = " + SOURCE_JSON_PATH + "\n")
                .append("Field Pinot Name = null\n")
                .append("Field Source DataType = null\n")
                .append("Field Destination DataType = null\n")
                .append("Field Conditional = false\n")
                .toString();

        assertEquals(expectedString, nullField.toString(), "toString output should handle null fields");
    }
}
