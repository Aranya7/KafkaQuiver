package com.lentra.realtime.entity.Field;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.when;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.config.store.ConfigStoreBuilder;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.exception.PipelineInitializationException;
import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;
import com.lentra.realtime.exception.PipelineConfiguationContentNotFoundException;

public class FieldBuilderTest {
    @Mock
    private Topic mockTopic;

    @Mock
    private Topic mockTopic2;

    private FieldBuilder sourceFieldBuilder;
    private FieldBuilder destinationFieldBuilder;

    private static final String TEST_CONFIG_PATH = "src/test/resources/test_mapping.properties";

    @BeforeEach
    void setUp() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException {
        MockitoAnnotations.openMocks(this);
        when(mockTopic.getTopicName()).thenReturn("loanCharges");
        when(mockTopic2.getTopicName()).thenReturn("dummy_target_topic");

        // Initialize the AppStore with test configuration
        AppStore.store = new ConfigStoreBuilder(TEST_CONFIG_PATH).build();

        sourceFieldBuilder = new FieldBuilder(mockTopic, EndPointType.SOURCE);
        destinationFieldBuilder = new FieldBuilder(mockTopic2, EndPointType.DESTINATION);
    }

    @Test
    void testGetSourceFields_Success() throws PipelineInitializationException {
        Set<Field> fields = sourceFieldBuilder.getFields();

        //should be equal to number of fields in mock topic (loanCharges).
        assertNotNull(fields);
        assertEquals(5, fields.size());

        //verify individual field properties
        Field field1 = fields.stream()
                .filter(f -> f.getPiontFieldName().equals("s_institution_id"))
                .findFirst()
                .orElse(null);

        assertNotNull(field1);
        assertEquals("$.after.institutionId", field1.getFieldSourceJsonPath());
        assertEquals("string", field1.getFieldSourceDataType());
        assertEquals("int", field1.getFieldDestinationDataType());
        assertFalse(field1.isConditional());

        //verifying another field
        Field field2 = fields.stream()
                .filter(f -> f.getPiontFieldName().equals("s_api_fees"))
                .findFirst()
                .orElse(null);

        assertNotNull(field2);
        assertEquals("$.after.feesDetails[?(@.feeType=='API charges'||@.feeType=='API CHARGES'||@.feeType=='apicharges'||@.feeType=='apiCharges')].fee", field2.getFieldSourceJsonPath());
        assertEquals("string", field2.getFieldSourceDataType());
        assertEquals("double", field2.getFieldDestinationDataType());
        assertTrue(field2.isConditional());
    }

    @Test
    void testGetDestinationFields_Success() throws PipelineInitializationException {
        Set<Field> fields = destinationFieldBuilder.getFields();

        assertNotNull(fields);
        assertFalse(fields.isEmpty());
        Field field1 = fields.stream()
                .filter(f -> f.getPiontFieldName().equals("dummy_mapping_1"))
                .findFirst()
                .orElse(null);
        assertNotNull(field1);
        assertEquals("$.dummy.mapping[1]", field1.getFieldSourceJsonPath());
        assertEquals("string", field1.getFieldSourceDataType());
        assertEquals("int", field1.getFieldDestinationDataType());
        assertFalse(field1.isConditional());
    }

    @Test
    void testGetSourceFields_NonExistentTopic() {
        when(mockTopic.getTopicName()).thenReturn("nonExistentTopic");

        assertThrows(PipelineInitializationException.class, () -> {
            sourceFieldBuilder.getFields();
        });
    }


    @Test
    void testEmptyTopicName() {
        when(mockTopic.getTopicName()).thenReturn("");

        assertThrows(PipelineInitializationException.class, () -> {
            sourceFieldBuilder.getFields();
        });
    }

    @Test
    void testTopicNameWithSpecialCharacters() {
        when(mockTopic.getTopicName()).thenReturn("loan$Charges@123");

        assertThrows(PipelineInitializationException.class, () -> {
            sourceFieldBuilder.getFields();
        });
    }


    @Test
    void testInvalidDataTypeConfiguration() {
        when(mockTopic.getTopicName()).thenReturn("invalid_datatype_topic");
        FieldBuilder invalidDatatypeBuilder = new FieldBuilder(mockTopic, EndPointType.SOURCE);

        assertThrows(PipelineInitializationException.class, () -> {
            invalidDatatypeBuilder.getFields();
        });
    }

//    @Test
//    void testDuplicateFieldNames() throws PipelineInitializationException {
//        when(mockTopic.getTopicName()).thenReturn("duplicate_fields_topic");
//        FieldBuilder duplicateFieldsBuilder = new FieldBuilder(mockTopic, EndPointType.SOURCE);
//
//        Set<Field> fields = duplicateFieldsBuilder.getFields();
//
//        // Verify that duplicate field names are handled appropriately
//        long distinctFieldNames = fields.stream()
//                .map(Field::getPiontFieldName)
//                .distinct()
//                .count();
//
//        assertEquals(fields.size(), distinctFieldNames, "Duplicate field names should be handled");
//    }


}
