package com.lentra.realtime.pipelines.impl;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.config.store.ConfigStoreBuilder;
import com.lentra.realtime.constants.PipelineConstants;
import com.lentra.realtime.entity.endpoint.AbstractEndPoint;
import com.lentra.realtime.entity.endpoint.DestinationEndPoint;
import com.lentra.realtime.entity.endpoint.EndPoint;
import com.lentra.realtime.entity.endpoint.SourceEndPoint;
import com.lentra.realtime.exception.*;
import com.lentra.realtime.pipelines.AbstractRealtimePipeline;
import com.lentra.realtime.pipelines.KStreamPipeline;
import com.lentra.realtime.pipelines.Pipeline;
import com.lentra.realtime.pipelines.impl.missioncontrol.MCKStreamImplementation;
import com.lentra.realtime.pipelines.impl.missioncontrol.MCKStreamImplementationTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.entity.Field.Field;
import com.lentra.realtime.factories.DataTypeConversionFactory;

import org.json.JSONObject;
import org.apache.kafka.streams.KafkaStreams;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class AbstractImplementationTest {

    public static StreamsBuilder builder;
    public static AbstractEndPoint sourceEndPoint;
    public static AbstractEndPoint destEndPoint;
    public static AbstractImplementation implementation;
    public static Pipeline pipeline;
    public static Object value;
    public static Topic topicObj;

    @Mock
    private Producer<String, String> producer;

    @Mock
    public DataTypeConversionFactory dataTypeConversionFactory;


    @BeforeAll
    public static void globalSetup() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException, PipelineInitializationException, PipelineBuildException {
        AppStore.store = new ConfigStoreBuilder("src/test/resources/test_mapping.properties").build();
        builder = new StreamsBuilder();
        sourceEndPoint = new SourceEndPoint();
        destEndPoint = new DestinationEndPoint();
        sourceEndPoint.initializeEndPoint();
        destEndPoint.initializeEndPoint();
        implementation = new MCKStreamImplementation();
        pipeline = new KStreamPipeline(implementation);
        pipeline.initialize();
        value = "{\"before\":null,\"after\":\"{\\\"_id\\\": \\\"R-PPL23121015745\\\",\\\"institutionId\\\": \\\"1001\\\",\\\"chargesDetails\\\": [],\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 3581.0,\\\"totalCountFees\\\": 3.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 14.0,\\\"totalsGstuGstAmount\\\": 7.29,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 95.0,\\\"totalFinancedFees\\\": 0.0,\\\"userDecision\\\": false},\\\"charges\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 0.0,\\\"totalCountFees\\\": 0.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 0.0,\\\"totalsGstuGstAmount\\\": 0.0},\\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0,\\\"totalGST\\\": 0.0,\\\"totalGrossPremioum\\\": 0.0,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 0.0,\\\"totalFinanced\\\": 0.0}},\\\"customObject\\\": {},\\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"calBasis\\\": \\\"Amount\\\",\\\"fee\\\": \\\"81.0\\\",\\\"taxAmount\\\": 14.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 95.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"feePercentage\\\": \\\"0\\\",\\\"tdsPercentage\\\": \\\"0\\\",\\\"grossTotalColm\\\": \\\"95.0\\\",\\\"method\\\": \\\"Forward\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 7.29,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"PROCESSING FEE\\\",\\\"calmethod\\\": \\\"Percentage\\\",\\\"calBasis\\\": \\\"Percentage\\\",\\\"fee\\\": \\\"3500.0\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feePercentage\\\": \\\"1.0\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"Stamp Duty\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"method\\\": \\\"Forward\\\",\\\"chargeCode\\\": \\\"10\\\",\\\"feePricingCrossDetails\\\": [],\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"date\\\": {\\\"$date\\\": 1704867852213},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 350.0,\\\"customObject\\\": {}}],\\\"insurenceCatList\\\": null,\\\"updatedDate\\\": {\\\"$date\\\": 1704867852222}}\",\"updateDescription\":{\"removedFields\":null,\"updatedFields\":\"{\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 3581.0, \\\"totalCountFees\\\": 3.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 14.0, \\\"totalsGstuGstAmount\\\": 7.29, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 95.0, \\\"totalFinancedFees\\\": 0.0, \\\"userDecision\\\": false}, \\\"charges\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 0.0, \\\"totalCountFees\\\": 0.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 0.0, \\\"totalsGstuGstAmount\\\": 0.0}, \\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0, \\\"totalGST\\\": 0.0, \\\"totalGrossPremioum\\\": 0.0, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 0.0, \\\"totalFinanced\\\": 0.0}}, \\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"calBasis\\\": \\\"Amount\\\", \\\"fee\\\": \\\"81.0\\\", \\\"taxAmount\\\": 14.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 95.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"feePercentage\\\": \\\"0\\\", \\\"tdsPercentage\\\": \\\"0\\\", \\\"grossTotalColm\\\": \\\"95.0\\\", \\\"method\\\": \\\"Forward\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 7.29, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"processingFees\\\", \\\"calmethod\\\": \\\"Percentage\\\", \\\"calBasis\\\": \\\"Percentage\\\", \\\"fee\\\": \\\"3500.0\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feePercentage\\\": \\\"1.0\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"Stamp Duty\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"method\\\": \\\"Forward\\\", \\\"chargeCode\\\": \\\"10\\\", \\\"feePricingCrossDetails\\\": [], \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"date\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.213Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 350.0, \\\"customObject\\\": {}}], \\\"updatedDate\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.222Z\\\"}}\",\"truncatedArrays\":null},\"source\":{\"version\":\"2.4.2.Final\",\"connector\":\"mongodb\",\"name\":\"flexi-uat-qa\",\"ts_ms\":1704887704000,\"snapshot\":\"false\",\"db\":\"flexi-gonogo\",\"sequence\":null,\"rs\":\"uat-mongodb\",\"collection\":\"loanCharges\",\"ord\":4,\"lsid\":null,\"txnNumber\":null,\"wallTime\":null},\"op\":\"u\",\"ts_ms\":1704867852326,\"transaction\":null}";
        topicObj = sourceEndPoint.getTopic("loanCharges");
    }

    @BeforeEach
    public void setUp() throws PipelineInitializationException {
        MockitoAnnotations.openMocks(this);

        sourceEndPoint = new SourceEndPoint();
        sourceEndPoint.initializeEndPoint();

        destEndPoint = new DestinationEndPoint();
        destEndPoint.initializeEndPoint();


        // Re-initialize the AbstractImplementation and inject the mock producer
        implementation = mock(AbstractImplementation.class, Mockito.CALLS_REAL_METHODS);
        implementation.producer = producer;  // Inject mock producer
    }

    @Test
    public void testInitialize() throws PipelineAuthenticationException {
        try {
			implementation.initialize(sourceEndPoint, destEndPoint, pipeline);
		} catch (EndPointConfigurationNotFound | PipelineAuthenticationException e) {
		}

        assertNotNull(implementation.source);
        assertNotNull(implementation.target);
        assertNotNull(implementation.pipeline);
        assertNotNull(implementation.producer);
        assertNotNull(AbstractImplementation.getDatatypeconversionfactory());
    }

    @Test
    public void testInitialize_TargetConfigException() {
        SourceEndPoint sourceMock = mock(SourceEndPoint.class);
        DestinationEndPoint targetMock = mock(DestinationEndPoint.class);
        Pipeline pipelineMock = mock(Pipeline.class);
        Producer<String, String> producerMock = mock(Producer.class);

        when(targetMock.getBootstrapURL()).thenThrow(new RuntimeException("Missing Bootstrap URL"));

        assertThrows(EndPointConfigurationNotFound.class, () -> {
            implementation.initialize(sourceMock, targetMock, pipelineMock);
        });

    }



    @Test
    void testDispatchDestinationRecord_Success() {
        String key = "testKey";
        String value = "testValue";
        String topic = "testTopic";

        ArgumentCaptor<ProducerRecord<String, String>> producerRecordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);

        implementation.dispatchDestinationRecord(key, value, topic);

        verify(producer, times(1)).send(producerRecordCaptor.capture(), any());

        ProducerRecord<String, String> capturedRecord = producerRecordCaptor.getValue();
        assertEquals(topic, capturedRecord.topic());
        assertEquals(key, capturedRecord.key());
        assertEquals(value, capturedRecord.value());
    }



    @Test
    void testDispatchDestinationRecord_ThrowableCaught() {
        String key = "testKey";
        String value = "testValue";
        String topic = "testTopic";

        doThrow(new RuntimeException("Test Throwable")).when(producer).send(any(), any());

        assertThrows(Exception.class, () -> implementation.dispatchDestinationRecord(key, value, topic));
    }

    @Test
    void testDispatchDestinationRecord_NullKey() {
        String value = "testValue";
        String topic = "testTopic";
        ArgumentCaptor<ProducerRecord<String, String>> producerRecordCaptor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        implementation.dispatchDestinationRecord(null, value, topic);

        verify(producer, times(1)).send(producerRecordCaptor.capture(), any());
        ProducerRecord<String, String> capturedRecord = producerRecordCaptor.getValue();
        assertNull(capturedRecord.key());
        assertEquals(value, capturedRecord.value());
    }


    @Test
    void testDispatchDestinationRecord_NullValue() {
        String key = "testKey";
        String topic = "testTopic";
        ArgumentCaptor<ProducerRecord<String, String>> producerRecordCaptor =
                ArgumentCaptor.forClass(ProducerRecord.class);

        implementation.dispatchDestinationRecord(key, null, topic);

        verify(producer, times(1)).send(producerRecordCaptor.capture(), any());
        ProducerRecord<String, String> capturedRecord = producerRecordCaptor.getValue();
        assertEquals(key, capturedRecord.key());
        assertNull(capturedRecord.value());
    }



    //Should work on Jatins Branch
    @Test
    public void testDispatchDestinationRecordWithNullTopic() {
                // Null topic
        assertThrows(Exception.class, () -> {
            implementation.dispatchDestinationRecord("key", "value", null);
        });
    }



    @Test
    public void testGetSourceKafkaRecord() throws PipelineBuildException, KStreamProcessingException {
        String sourceRecord = "sourceRecord";
        Topic topicObj = sourceEndPoint.getTopic("loanCharges");


        JSONObject result = implementation.getSourceKafkaRecord(topicObj, value);

        assertNotNull(result);

        ClassLoader classLoader = MCKStreamImplementationTest.class.getClassLoader();
        InputStream schemaStream = classLoader.getResourceAsStream("sourceMappedRecord.bin");

        JSONObject sourceKafkaRecord = new JSONObject(convertStreamToString(schemaStream));
        assertEquals(result.toString(), sourceKafkaRecord.toString());
    }


    @Test
    public void testBuildDestinationValue() throws KStreamProcessingException {
        Field mockField = mock(Field.class);
        JSONObject sourceKafkaRecord = implementation.getSourceKafkaRecord(topicObj, value);
        JSONObject result = implementation.buildDestinationValue(sourceKafkaRecord, topicObj);

        assertNotNull(result);
        System.out.println("Res: "+result);
    }

    @Test
    public void testBuildDestinationValueWithInvalidSourceRecord() {
        JSONObject invalidSourceRecord = new JSONObject();  // Empty or invalid source record
        JSONObject result = implementation.buildDestinationValue(invalidSourceRecord, topicObj);

        assertNotNull(result);
        assertTrue(result.isEmpty(), "Result should be empty for invalid source record");
    }

//
//    @Test
//    public void testBuildDestinationValue2() {
//        JSONObject sourceKafkaRecord = implementation.getSourceKafkaRecord(topicObj, value);
////        sourceKafkaRecord.put("institutionId", "1001");
//
//        when(implementation.getSourceKafkaRecord(any(), any())).thenReturn(sourceKafkaRecord);
//
//        JSONObject destinationValue = implementation.buildDestinationValue(sourceKafkaRecord, topicObj);
//
//        assertNotNull(destinationValue, "Destination value should not be null");
//        assertEquals("1001", destinationValue.getString("institutionId"));
//    }

    @Test
    public void testGetKafkaStream() throws PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException, PipelineBuildException {
        implementation.initialize(sourceEndPoint, destEndPoint, pipeline);
        implementation.configureSourceEndPointForStreaming();
        KafkaStreams teststreams = implementation.getKafkaStream(sourceEndPoint.getUserDefinedTopicList());
        assertNotNull(teststreams);
    }

    @Test
    public void testGetKafkaStreamWithEmptyTopicList() throws PipelineAuthenticationException, EndPointConfigurationNotFound {
        implementation.initialize(sourceEndPoint, destEndPoint, pipeline);
        implementation.configureSourceEndPointForStreaming();

        //Mentionn exception after jatins changes
        assertThrows(Exception.class, () -> {
            implementation.getKafkaStream(null);
        });

        assertThrows(Exception.class, () -> {
            implementation.getKafkaStream(Arrays.asList());
        });
    }


    @Test
    public void testApplyDataTypeConversion() throws KStreamProcessingException, PipelineInitializationException {
        Field testField = new Field("$.after.institutionId", "s_institution_id", "string", "int", false);
        Field testField2 = new Field("$.after.institutionId", "s_institution_id", null, null, false);
        Field testField3 = new Field("$.after.institutionId", "s_institution_id", "strang", "yint", false);

        Object myObject = "1001";
        Object convertedValue = implementation.applyDataTypeConversion(myObject, testField);
        Object convertedValue2 = implementation.applyDataTypeConversion(myObject, testField2);
        assertEquals(1001, convertedValue);
        assertEquals("1001", convertedValue2);

        //assertEquals("1001",implementation.applyDataTypeConversion(myObject, testField3), "Unsupported data types should return the original value");
    }

    //Test how applyDataTypeConversion behaves when the Field object is null.
    @Test
    public void testApplyDataTypeConversionWithNullField() throws KStreamProcessingException {
        //Object result = implementation.applyDataTypeConversion("value", null);
        assertThrows(NullPointerException.class, () -> {
            implementation.applyDataTypeConversion("value", null);
        });
    }

    @Test
    public void testGetProducer(){
        assertNotNull(implementation.getProducer());
    }

    @Test
    public void testConfigureSourceEndPointForStreaming_Success() throws PipelineAuthenticationException, EndPointConfigurationNotFound {
        implementation.initialize(sourceEndPoint, destEndPoint, pipeline);

        Properties actualSourceConfs = implementation.configureSourceEndPointForStreaming();

        assertEquals("test_app", actualSourceConfs.get("application.id"));
        assertEquals("localhost:9092", actualSourceConfs.get("bootstrap.servers"));
        assertEquals("earliest", actualSourceConfs.get("auto.offset.reset"));
        assertEquals("SASL_SSL", actualSourceConfs.get("security.protocol"));
        assertEquals("software.amazon.msk.auth.iam.IAMLoginModule required;", actualSourceConfs.get("sasl.jaas.config"));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", actualSourceConfs.get("sasl.client.callback.handler.class"));
    }

    @Test
    public void testConfigureDestinationEndPointForStreaming_Success() throws PipelineAuthenticationException {

        Properties actualDestConfs = null;
		try {
			implementation.initialize(sourceEndPoint, destEndPoint, pipeline);
			actualDestConfs = implementation.configureDestinationEndPointForStreaming();
		} catch (PipelineAuthenticationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (EndPointConfigurationNotFound e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println(actualDestConfs);

        assertEquals("localhost:9092", actualDestConfs.get("bootstrap.servers"));
        assertEquals("SASL_SSL", actualDestConfs.get("security.protocol"));
        assertEquals("software.amazon.msk.auth.iam.IAMLoginModule required;", actualDestConfs.get("sasl.jaas.config"));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", actualDestConfs.get("sasl.client.callback.handler.class"));
    }

    @Test
    public void testConfigureDestinationEndPointForStreaming_NullBootstrapURL() {
        DestinationEndPoint mockTarget = mock(DestinationEndPoint.class);
        implementation.target = mockTarget;

        when(mockTarget.getBootstrapURL()).thenReturn(null);

        assertThrows(EndPointConfigurationNotFound.class, () -> {
            implementation.configureDestinationEndPointForStreaming();
        });
    }


//    @Test
//    public void testConfigureSourceEndPointForStreaming_Failure() throws PipelineAuthenticationException, EndPointConfigurationNotFound {
//        implementation.initialize(sourceEndPoint, destEndPoint, pipeline);
//
//        Properties actualSourceConfs = implementation.configureSourceEndPointForStreaming();
//
//        //AppStore.store = new ConfigStoreBuilder("/home/aranyasinghchauhan/Videos/MC_Repo_New/insights-reports/New_MCKStreamRealtime/MCKStreamRealtime/src/test/resources/test_mapping.properties").build();
//        PipelineConstants pc = mock(PipelineConstants.class);
//        when(pc.PIPELINE_APP_NAME_MAPPING_KEY).thenThrow(new NullPointerException("Missing App Name"));
//
//        assertEquals("test_app", actualSourceConfs.get("application.id"));
//        assertEquals("localhost:9092", actualSourceConfs.get("bootstrap.servers"));
//        assertEquals("earliest", actualSourceConfs.get("auto.offset.reset"));
//        assertEquals("SASL_SSL", actualSourceConfs.get("security.protocol"));
//        assertEquals("software.amazon.msk.auth.iam.IAMLoginModule required;", actualSourceConfs.get("sasl.jaas.config"));
//        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", actualSourceConfs.get("sasl.client.callback.handler.class"));
//    }

//

    @Test
    public void testImplement_Success() throws PipelineBuildException, PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException {
        KafkaStreams mockStreams = mock(KafkaStreams.class);
        SourceEndPoint mockSourceEndpoint = mock(SourceEndPoint.class);
        List<String> mockTopicList = Arrays.asList("topic1", "topic2");

        implementation.source = mockSourceEndpoint;
        when(mockSourceEndpoint.getUserDefinedTopicList()).thenReturn(mockTopicList);

        AbstractImplementation spyImplementation = spy(implementation);
        doReturn(mockStreams).when(spyImplementation).getKafkaStream(mockTopicList);

        spyImplementation.implement();

        verify(mockStreams, times(1)).start();
    }

    @Test
    public void testImplement_NullTopicList() throws PipelineAuthenticationException, EndPointConfigurationNotFound {
        SourceEndPoint mockSourceEndpoint = mock(SourceEndPoint.class);
        implementation.source = mockSourceEndpoint;
        when(mockSourceEndpoint.getUserDefinedTopicList()).thenReturn(null);

        assertThrows(KafkaTopicNotFoundException.class, () -> {
            implementation.implement();
        });
    }

    @Test
    public void testImplement_EmptyTopicList() throws PipelineAuthenticationException, EndPointConfigurationNotFound {
        SourceEndPoint mockSourceEndpoint = mock(SourceEndPoint.class);
        implementation.source = mockSourceEndpoint;
        when(mockSourceEndpoint.getUserDefinedTopicList()).thenReturn(Collections.emptyList());

        assertThrows(KafkaTopicNotFoundException.class, () -> {
            implementation.implement();
        });
    }

    @Test
    public void testImplement_AuthenticationFailure() throws PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException, PipelineBuildException {
        SourceEndPoint mockSourceEndpoint = mock(SourceEndPoint.class);
        List<String> mockTopicList = Arrays.asList("topic1", "topic2");

        implementation.source = mockSourceEndpoint;
        when(mockSourceEndpoint.getUserDefinedTopicList()).thenReturn(mockTopicList);

        AbstractImplementation spyImplementation = spy(implementation);
        doThrow(new PipelineAuthenticationException("Authentication failed"))
                .when(spyImplementation).getKafkaStream(mockTopicList);

        assertThrows(PipelineAuthenticationException.class, () -> {
            spyImplementation.implement();
        });
    }

    @Test
    public void testImplement_StreamBuildFailure() throws PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException, PipelineBuildException {
        SourceEndPoint mockSourceEndpoint = mock(SourceEndPoint.class);
        List<String> mockTopicList = Arrays.asList("topic1", "topic2");

        implementation.source = mockSourceEndpoint;
        when(mockSourceEndpoint.getUserDefinedTopicList()).thenReturn(mockTopicList);

        AbstractImplementation spyImplementation = spy(implementation);
        doThrow(new PipelineBuildException("Failed to build stream"))
                .when(spyImplementation).getKafkaStream(mockTopicList);

        assertThrows(PipelineBuildException.class, () -> {
            spyImplementation.implement();
        });
    }

    @Test
    public void testImplement_RuntimeException() throws PipelineAuthenticationException, EndPointConfigurationNotFound, KafkaTopicNotFoundException, PipelineBuildException {
        SourceEndPoint mockSourceEndpoint = mock(SourceEndPoint.class);
        KafkaStreams mockStreams = mock(KafkaStreams.class);
        List<String> mockTopicList = Arrays.asList("topic1", "topic2");

        implementation.source = mockSourceEndpoint;
        when(mockSourceEndpoint.getUserDefinedTopicList()).thenReturn(mockTopicList);

        AbstractImplementation spyImplementation = spy(implementation);
        doReturn(mockStreams).when(spyImplementation).getKafkaStream(mockTopicList);
        doThrow(new RuntimeException("Stream start failed")).when(mockStreams).start();

        assertThrows(RuntimeException.class, () -> {
            spyImplementation.implement();
        });
    }
    private static String convertStreamToString(InputStream is) {
        Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name());
        String text = scanner.useDelimiter("\\A").next();
        scanner.close();
        return text;
    }


}
