package com.lentra.realtime.pipelines.impl.missioncontrol;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.config.store.ConfigStoreBuilder;
import com.lentra.realtime.entity.Field.Field;
import com.lentra.realtime.entity.Topic.Topic;
import com.lentra.realtime.entity.endpoint.AbstractEndPoint;
import com.lentra.realtime.entity.endpoint.DestinationEndPoint;
import com.lentra.realtime.entity.endpoint.SourceEndPoint;
import com.lentra.realtime.exception.*;
import com.lentra.realtime.factories.DataTypeConversionFactory;
import com.lentra.realtime.pipelines.KStreamPipeline;
import com.lentra.realtime.pipelines.Pipeline;
import com.lentra.realtime.pipelines.impl.AbstractImplementation;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MCKStreamImplementationTest {
    public static StreamsBuilder builder;
    public static AbstractEndPoint sourceEndPoint;
    public static AbstractEndPoint destEndPoint;
    public static AbstractImplementation mc_implementation;
    public static Pipeline pipeline;
    public static Object value;
    public static Object value2;
    public static Object value_wo_app_id;
    public static Topic topicObj;
    public static Object key;
    public static JSONObject sourceKafkaRecord;

    @Mock
    private Topic mockTopic;

    @Mock
    private Producer<String, String> producer2;

    private MCKStreamImplementation processor;

    @BeforeAll
    public static void globalSetup() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException, PipelineInitializationException, PipelineBuildException, PipelineAuthenticationException, EndPointConfigurationNotFound {
        AppStore.store = new ConfigStoreBuilder("src/test/resources/test_mapping.properties").build();
        builder = new StreamsBuilder();
        sourceEndPoint = new SourceEndPoint();
        destEndPoint = new DestinationEndPoint();
        sourceEndPoint.initializeEndPoint();
        destEndPoint.initializeEndPoint();
        mc_implementation = new MCKStreamImplementation();

        pipeline = new KStreamPipeline(mc_implementation);
        pipeline.initialize();
        try {
			mc_implementation.initialize(sourceEndPoint, destEndPoint, pipeline);
		} catch (EndPointConfigurationNotFound e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PipelineAuthenticationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        value = "{\"before\":null,\"after\":\"{\\\"_id\\\": \\\"R-PPL23121015745\\\",\\\"institutionId\\\": \\\"1001\\\",\\\"chargesDetails\\\": [],\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 3581.0,\\\"totalCountFees\\\": 3.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 14.0,\\\"totalsGstuGstAmount\\\": 7.29,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 95.0,\\\"totalFinancedFees\\\": 0.0,\\\"userDecision\\\": false},\\\"charges\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 0.0,\\\"totalCountFees\\\": 0.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 0.0,\\\"totalsGstuGstAmount\\\": 0.0},\\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0,\\\"totalGST\\\": 0.0,\\\"totalGrossPremioum\\\": 0.0,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 0.0,\\\"totalFinanced\\\": 0.0}},\\\"customObject\\\": {},\\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"calBasis\\\": \\\"Amount\\\",\\\"fee\\\": \\\"81.0\\\",\\\"taxAmount\\\": 14.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 95.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"feePercentage\\\": \\\"0\\\",\\\"tdsPercentage\\\": \\\"0\\\",\\\"grossTotalColm\\\": \\\"95.0\\\",\\\"method\\\": \\\"Forward\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 7.29,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"PROCESSING FEE\\\",\\\"calmethod\\\": \\\"Percentage\\\",\\\"calBasis\\\": \\\"Percentage\\\",\\\"fee\\\": \\\"3500.0\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feePercentage\\\": \\\"1.0\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"Stamp Duty\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"method\\\": \\\"Forward\\\",\\\"chargeCode\\\": \\\"10\\\",\\\"feePricingCrossDetails\\\": [],\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"date\\\": {\\\"$date\\\": 1704867852213},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 350.0,\\\"customObject\\\": {}}],\\\"insurenceCatList\\\": null,\\\"updatedDate\\\": {\\\"$date\\\": 1704867852222}}\",\"updateDescription\":{\"removedFields\":null,\"updatedFields\":\"{\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 3581.0, \\\"totalCountFees\\\": 3.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 14.0, \\\"totalsGstuGstAmount\\\": 7.29, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 95.0, \\\"totalFinancedFees\\\": 0.0, \\\"userDecision\\\": false}, \\\"charges\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 0.0, \\\"totalCountFees\\\": 0.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 0.0, \\\"totalsGstuGstAmount\\\": 0.0}, \\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0, \\\"totalGST\\\": 0.0, \\\"totalGrossPremioum\\\": 0.0, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 0.0, \\\"totalFinanced\\\": 0.0}}, \\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"calBasis\\\": \\\"Amount\\\", \\\"fee\\\": \\\"81.0\\\", \\\"taxAmount\\\": 14.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 95.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"feePercentage\\\": \\\"0\\\", \\\"tdsPercentage\\\": \\\"0\\\", \\\"grossTotalColm\\\": \\\"95.0\\\", \\\"method\\\": \\\"Forward\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 7.29, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"processingFees\\\", \\\"calmethod\\\": \\\"Percentage\\\", \\\"calBasis\\\": \\\"Percentage\\\", \\\"fee\\\": \\\"3500.0\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feePercentage\\\": \\\"1.0\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"Stamp Duty\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"method\\\": \\\"Forward\\\", \\\"chargeCode\\\": \\\"10\\\", \\\"feePricingCrossDetails\\\": [], \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"date\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.213Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 350.0, \\\"customObject\\\": {}}], \\\"updatedDate\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.222Z\\\"}}\",\"truncatedArrays\":null},\"source\":{\"version\":\"2.4.2.Final\",\"connector\":\"mongodb\",\"name\":\"flexi-uat-qa\",\"ts_ms\":1704887704000,\"snapshot\":\"false\",\"db\":\"flexi-gonogo\",\"sequence\":null,\"rs\":\"uat-mongodb\",\"collection\":\"loanCharges\",\"ord\":4,\"lsid\":null,\"txnNumber\":null,\"wallTime\":null},\"op\":\"u\",\"ts_ms\":1704867852326,\"transaction\":null}";
        value2 = "{\"before\":null,\"after\":\"{\\\"_id\\\":null,\\\"institutionId\\\": \\\"1001\\\",\\\"chargesDetails\\\": [],\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 3581.0,\\\"totalCountFees\\\": 3.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 14.0,\\\"totalsGstuGstAmount\\\": 7.29,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 95.0,\\\"totalFinancedFees\\\": 0.0,\\\"userDecision\\\": false},\\\"charges\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 0.0,\\\"totalCountFees\\\": 0.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 0.0,\\\"totalsGstuGstAmount\\\": 0.0},\\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0,\\\"totalGST\\\": 0.0,\\\"totalGrossPremioum\\\": 0.0,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 0.0,\\\"totalFinanced\\\": 0.0}},\\\"customObject\\\": {},\\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"calBasis\\\": \\\"Amount\\\",\\\"fee\\\": \\\"81.0\\\",\\\"taxAmount\\\": 14.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 95.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"feePercentage\\\": \\\"0\\\",\\\"tdsPercentage\\\": \\\"0\\\",\\\"grossTotalColm\\\": \\\"95.0\\\",\\\"method\\\": \\\"Forward\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 7.29,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"PROCESSING FEE\\\",\\\"calmethod\\\": \\\"Percentage\\\",\\\"calBasis\\\": \\\"Percentage\\\",\\\"fee\\\": \\\"3500.0\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feePercentage\\\": \\\"1.0\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"Stamp Duty\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"method\\\": \\\"Forward\\\",\\\"chargeCode\\\": \\\"10\\\",\\\"feePricingCrossDetails\\\": [],\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"date\\\": {\\\"$date\\\": 1704867852213},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 350.0,\\\"customObject\\\": {}}],\\\"insurenceCatList\\\": null,\\\"updatedDate\\\": {\\\"$date\\\": 1704867852222}}\",\"updateDescription\":{\"removedFields\":null,\"updatedFields\":\"{\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 3581.0, \\\"totalCountFees\\\": 3.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 14.0, \\\"totalsGstuGstAmount\\\": 7.29, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 95.0, \\\"totalFinancedFees\\\": 0.0, \\\"userDecision\\\": false}, \\\"charges\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 0.0, \\\"totalCountFees\\\": 0.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 0.0, \\\"totalsGstuGstAmount\\\": 0.0}, \\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0, \\\"totalGST\\\": 0.0, \\\"totalGrossPremioum\\\": 0.0, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 0.0, \\\"totalFinanced\\\": 0.0}}, \\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"calBasis\\\": \\\"Amount\\\", \\\"fee\\\": \\\"81.0\\\", \\\"taxAmount\\\": 14.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 95.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"feePercentage\\\": \\\"0\\\", \\\"tdsPercentage\\\": \\\"0\\\", \\\"grossTotalColm\\\": \\\"95.0\\\", \\\"method\\\": \\\"Forward\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 7.29, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"processingFees\\\", \\\"calmethod\\\": \\\"Percentage\\\", \\\"calBasis\\\": \\\"Percentage\\\", \\\"fee\\\": \\\"3500.0\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feePercentage\\\": \\\"1.0\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"Stamp Duty\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"method\\\": \\\"Forward\\\", \\\"chargeCode\\\": \\\"10\\\", \\\"feePricingCrossDetails\\\": [], \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"date\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.213Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 350.0, \\\"customObject\\\": {}}], \\\"updatedDate\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.222Z\\\"}}\",\"truncatedArrays\":null},\"source\":{\"version\":\"2.4.2.Final\",\"connector\":\"mongodb\",\"name\":\"flexi-uat-qa\",\"ts_ms\":1704887704000,\"snapshot\":\"false\",\"db\":\"flexi-gonogo\",\"sequence\":null,\"rs\":\"uat-mongodb\",\"collection\":\"loanCharges\",\"ord\":4,\"lsid\":null,\"txnNumber\":null,\"wallTime\":null},\"op\":\"u\",\"ts_ms\":1704867852326,\"transaction\":null}";
        value_wo_app_id = "{\"before\":null,\"after\":\"{\\\"institutionId\\\": \\\"1001\\\",\\\"chargesDetails\\\": [],\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 3581.0,\\\"totalCountFees\\\": 3.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 14.0,\\\"totalsGstuGstAmount\\\": 7.29,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 95.0,\\\"totalFinancedFees\\\": 0.0,\\\"userDecision\\\": false},\\\"charges\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 0.0,\\\"totalCountFees\\\": 0.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 0.0,\\\"totalsGstuGstAmount\\\": 0.0},\\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0,\\\"totalGST\\\": 0.0,\\\"totalGrossPremioum\\\": 0.0,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 0.0,\\\"totalFinanced\\\": 0.0}},\\\"customObject\\\": {},\\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"calBasis\\\": \\\"Amount\\\",\\\"fee\\\": \\\"81.0\\\",\\\"taxAmount\\\": 14.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 95.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"feePercentage\\\": \\\"0\\\",\\\"tdsPercentage\\\": \\\"0\\\",\\\"grossTotalColm\\\": \\\"95.0\\\",\\\"method\\\": \\\"Forward\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 7.29,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"PROCESSING FEE\\\",\\\"calmethod\\\": \\\"Percentage\\\",\\\"calBasis\\\": \\\"Percentage\\\",\\\"fee\\\": \\\"3500.0\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feePercentage\\\": \\\"1.0\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"Stamp Duty\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"method\\\": \\\"Forward\\\",\\\"chargeCode\\\": \\\"10\\\",\\\"feePricingCrossDetails\\\": [],\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"date\\\": {\\\"$date\\\": 1704867852213},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 350.0,\\\"customObject\\\": {}}],\\\"insurenceCatList\\\": null,\\\"updatedDate\\\": {\\\"$date\\\": 1704867852222}}\",\"updateDescription\":{\"removedFields\":null,\"updatedFields\":\"{\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 3581.0, \\\"totalCountFees\\\": 3.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 14.0, \\\"totalsGstuGstAmount\\\": 7.29, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 95.0, \\\"totalFinancedFees\\\": 0.0, \\\"userDecision\\\": false}, \\\"charges\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 0.0, \\\"totalCountFees\\\": 0.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 0.0, \\\"totalsGstuGstAmount\\\": 0.0}, \\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0, \\\"totalGST\\\": 0.0, \\\"totalGrossPremioum\\\": 0.0, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 0.0, \\\"totalFinanced\\\": 0.0}}, \\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"calBasis\\\": \\\"Amount\\\", \\\"fee\\\": \\\"81.0\\\", \\\"taxAmount\\\": 14.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 95.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"feePercentage\\\": \\\"0\\\", \\\"tdsPercentage\\\": \\\"0\\\", \\\"grossTotalColm\\\": \\\"95.0\\\", \\\"method\\\": \\\"Forward\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 7.29, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"processingFees\\\", \\\"calmethod\\\": \\\"Percentage\\\", \\\"calBasis\\\": \\\"Percentage\\\", \\\"fee\\\": \\\"3500.0\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feePercentage\\\": \\\"1.0\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"Stamp Duty\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"method\\\": \\\"Forward\\\", \\\"chargeCode\\\": \\\"10\\\", \\\"feePricingCrossDetails\\\": [], \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"date\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.213Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 350.0, \\\"customObject\\\": {}}], \\\"updatedDate\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.222Z\\\"}}\",\"truncatedArrays\":null},\"source\":{\"version\":\"2.4.2.Final\",\"connector\":\"mongodb\",\"name\":\"flexi-uat-qa\",\"ts_ms\":1704887704000,\"snapshot\":\"false\",\"db\":\"flexi-gonogo\",\"sequence\":null,\"rs\":\"uat-mongodb\",\"collection\":\"loanCharges\",\"ord\":4,\"lsid\":null,\"txnNumber\":null,\"wallTime\":null},\"op\":\"u\",\"ts_ms\":1704867852326,\"transaction\":null}";
        topicObj = sourceEndPoint.getTopic("loanCharges");
        key=null;
        //sourceKafkaRecord = "{\"op\":\"u\",\"before\":null,\"updateDescription\":{\"updatedFields\":\"{\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 3581.0, \\\"totalCountFees\\\": 3.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 14.0, \\\"totalsGstuGstAmount\\\": 7.29, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 95.0, \\\"totalFinancedFees\\\": 0.0, \\\"userDecision\\\": false}, \\\"charges\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 0.0, \\\"totalCountFees\\\": 0.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 0.0, \\\"totalsGstuGstAmount\\\": 0.0}, \\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0, \\\"totalGST\\\": 0.0, \\\"totalGrossPremioum\\\": 0.0, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 0.0, \\\"totalFinanced\\\": 0.0}}, \\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"calBasis\\\": \\\"Amount\\\", \\\"fee\\\": \\\"81.0\\\", \\\"taxAmount\\\": 14.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 95.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"feePercentage\\\": \\\"0\\\", \\\"tdsPercentage\\\": \\\"0\\\", \\\"grossTotalColm\\\": \\\"95.0\\\", \\\"method\\\": \\\"Forward\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 7.29, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"processingFees\\\", \\\"calmethod\\\": \\\"Percentage\\\", \\\"calBasis\\\": \\\"Percentage\\\", \\\"fee\\\": \\\"3500.0\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feePercentage\\\": \\\"1.0\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"Stamp Duty\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"method\\\": \\\"Forward\\\", \\\"chargeCode\\\": \\\"10\\\", \\\"feePricingCrossDetails\\\": [], \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"date\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.213Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 350.0, \\\"customObject\\\": {}}], \\\"updatedDate\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.222Z\\\"}}\",\"truncatedArrays\":null,\"removedFields\":null},\"after\":{\"insurenceCatList\":null,\"institutionId\":\"1001\",\"feesDetails\":[{\"calmethod\":\"Amount\",\"calculationType\":\"Flat\",\"taxApplicable\":\"yes\",\"date\":{\"$date\":1703670946527},\"fee\":\"81.0\",\"isActive\":\"true\",\"mandatory\":\"false\",\"feePricingCrossDetails\":[],\"total\":95,\"customObject\":{},\"lockInPeriod\":0,\"tdsPercentage\":\"0\",\"iGstRate\":\"0.0\",\"dsaApiTriggered\":false,\"method\":\"Forward\",\"iGstAmount\":0,\"feePercentage\":\"0\",\"feeType\":\"apiCharges\",\"chargeTypeCode\":\"logfeetype\",\"sGstuGstRate\":\"9.0\",\"totalAmount\":0,\"ugstSgstAmount\":7.29,\"feeRecoveryMode\":\"Deduct from Disb\",\"tdsAmount\":0,\"grossTotalColm\":\"95.0\",\"calBasis\":\"Amount\",\"taxAmount\":14,\"chargeCode\":\"500355.0\"},{\"calmethod\":\"Percentage\",\"calculationType\":\"Flat\",\"taxApplicable\":\"yes\",\"date\":{\"$date\":1703670946527},\"iGstRate\":\"0.0\",\"dsaApiTriggered\":false,\"iGstAmount\":0,\"feePercentage\":\"1.0\",\"fee\":\"3500.0\",\"feeType\":\"PROCESSING FEE\",\"isActive\":\"true\",\"chargeTypeCode\":\"logfeetype\",\"mandatory\":\"false\",\"sGstuGstRate\":\"9.0\",\"feePricingCrossDetails\":[],\"totalAmount\":0,\"total\":0,\"ugstSgstAmount\":0,\"tdsAmount\":0,\"customObject\":{},\"lockInPeriod\":0,\"calBasis\":\"Percentage\",\"taxAmount\":0,\"chargeCode\":\"500355.0\"},{\"calmethod\":\"Amount\",\"date\":{\"$date\":1704867852213},\"dsaApiTriggered\":false,\"method\":\"Forward\",\"feeType\":\"Stamp Duty\",\"feePricingCrossDetails\":[],\"totalAmount\":350,\"total\":0,\"ugstSgstAmount\":0,\"feeRecoveryMode\":\"Deduct from Disb\",\"tdsAmount\":0,\"customObject\":{},\"lockInPeriod\":0,\"taxAmount\":0,\"chargeCode\":\"10\"}],\"chargesDetails\":[],\"customObject\":{},\"_id\":\"R-PPL23121015745\",\"updatedDate\":{\"$date\":1704867852222},\"chargesSummary\":{\"insurance\":{\"totalDeductedfromDisb\":0,\"totalGST\":0,\"totalInitialReciept\":0,\"totalFinanced\":0,\"totalNetPremioum\":0,\"totalGrossPremioum\":0},\"fees\":{\"totalTdsAmount\":0,\"totalFees\":3581,\"totalIGst\":0,\"totalDeductedfromDisb\":95,\"userDecision\":false,\"totalInitialReciept\":0,\"totalCountFees\":3,\"totalsGstuGstAmount\":7.29,\"totalTaxAmount\":14,\"totalFinancedFees\":0},\"charges\":{\"totalTdsAmount\":0,\"totalFees\":0,\"totalIGst\":0,\"totalCountFees\":0,\"totalsGstuGstAmount\":0,\"totalTaxAmount\":0}}},\"source\":{\"ord\":4,\"rs\":\"uat-mongodb\",\"txnNumber\":null,\"collection\":\"loanCharges\",\"version\":\"2.4.2.Final\",\"lsid\":null,\"sequence\":null,\"connector\":\"mongodb\",\"name\":\"flexi-uat-qa\",\"wallTime\":null,\"ts_ms\":1704887704000,\"snapshot\":\"false\",\"db\":\"flexi-gonogo\"},\"ts_ms\":1704867852326,\"transaction\":null}";

        ClassLoader classLoader = MCKStreamImplementationTest.class.getClassLoader();
        InputStream schemaStream = classLoader.getResourceAsStream("sourceMappedRecord.bin");

        if (schemaStream != null) {
            sourceKafkaRecord = new JSONObject(convertStreamToString(schemaStream));
        } else {
            System.err.println("sourceMappedRecord.bin not found in resources.");
        }
    }

    private static String convertStreamToString(InputStream is) {
        Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name());
        String text = scanner.useDelimiter("\\A").next();
        scanner.close();
        return text;
    }

    @Test
    void testProcess_ValidMessage() throws PipelineAuthenticationException, KStreamProcessingException {
        mc_implementation = Mockito.spy(new MCKStreamImplementation());

        doNothing().when(mc_implementation).dispatchDestinationRecord(any(), any(), any());

        KeyValue<Object, Object> kv = mc_implementation.process("loanCharges", key, value, topicObj);

        assertEquals("R-PPL23121015745", (kv.key).toString());
        String expected1 = "{\"s_api_fees\":81,\"s_application_id\":\"R-PPL23121015745\",\"s_institution_id\":1001,\"ts_ms\":1704887704000}";
        String expected2 = "{\"s_api_fees\":81,\"s_application_id\":\"R-PPL23121015745\",\"ts_ms\":1704887704000,\"s_institution_id\":1001}";

        assertTrue((kv.value).toString().equals(expected1) || (kv.value).toString().equals(expected2));
        //assertEquals("{\"s_api_fees\":81,\"s_application_id\":\"R-PPL23121015745\",\"ts_ms\":1704887704000,\"s_institution_id\":1001}", (kv.value).toString());
    }


    @Test
    void testProcess_InvalidMessage() {
        mc_implementation = Mockito.spy(new MCKStreamImplementation());
        doNothing().when(mc_implementation).dispatchDestinationRecord(any(), any(), any());
        System.out.println("1");
        assertThrows(Exception.class, () -> {
            mc_implementation.process("loanCharges", key, value2, topicObj);
        });


        Object valueWithOid = "{\"before\":null,\"after\":\"{\\\"_id\\\":{\\\"$oid\\\": \\\"587491da5da0a1278da476e0\\\"},\\\"institutionId\\\": \\\"1001\\\",\\\"chargesDetails\\\": [],\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 3581.0,\\\"totalCountFees\\\": 3.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 14.0,\\\"totalsGstuGstAmount\\\": 7.29,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 95.0,\\\"totalFinancedFees\\\": 0.0,\\\"userDecision\\\": false},\\\"charges\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 0.0,\\\"totalCountFees\\\": 0.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 0.0,\\\"totalsGstuGstAmount\\\": 0.0},\\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0,\\\"totalGST\\\": 0.0,\\\"totalGrossPremioum\\\": 0.0,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 0.0,\\\"totalFinanced\\\": 0.0}},\\\"customObject\\\": {},\\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"calBasis\\\": \\\"Amount\\\",\\\"fee\\\": \\\"81.0\\\",\\\"taxAmount\\\": 14.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 95.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"feePercentage\\\": \\\"0\\\",\\\"tdsPercentage\\\": \\\"0\\\",\\\"grossTotalColm\\\": \\\"95.0\\\",\\\"method\\\": \\\"Forward\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 7.29,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"PROCESSING FEE\\\",\\\"calmethod\\\": \\\"Percentage\\\",\\\"calBasis\\\": \\\"Percentage\\\",\\\"fee\\\": \\\"3500.0\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feePercentage\\\": \\\"1.0\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"Stamp Duty\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"method\\\": \\\"Forward\\\",\\\"chargeCode\\\": \\\"10\\\",\\\"feePricingCrossDetails\\\": [],\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"date\\\": {\\\"$date\\\": 1704867852213},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 350.0,\\\"customObject\\\": {}}],\\\"insurenceCatList\\\": null,\\\"updatedDate\\\": {\\\"$date\\\": 1704867852222}}\",\"updateDescription\":{\"removedFields\":null,\"updatedFields\":\"{\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 3581.0, \\\"totalCountFees\\\": 3.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 14.0, \\\"totalsGstuGstAmount\\\": 7.29, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 95.0, \\\"totalFinancedFees\\\": 0.0, \\\"userDecision\\\": false}, \\\"charges\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 0.0, \\\"totalCountFees\\\": 0.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 0.0, \\\"totalsGstuGstAmount\\\": 0.0}, \\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0, \\\"totalGST\\\": 0.0, \\\"totalGrossPremioum\\\": 0.0, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 0.0, \\\"totalFinanced\\\": 0.0}}, \\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"calBasis\\\": \\\"Amount\\\", \\\"fee\\\": \\\"81.0\\\", \\\"taxAmount\\\": 14.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 95.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"feePercentage\\\": \\\"0\\\", \\\"tdsPercentage\\\": \\\"0\\\", \\\"grossTotalColm\\\": \\\"95.0\\\", \\\"method\\\": \\\"Forward\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 7.29, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"processingFees\\\", \\\"calmethod\\\": \\\"Percentage\\\", \\\"calBasis\\\": \\\"Percentage\\\", \\\"fee\\\": \\\"3500.0\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feePercentage\\\": \\\"1.0\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"Stamp Duty\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"method\\\": \\\"Forward\\\", \\\"chargeCode\\\": \\\"10\\\", \\\"feePricingCrossDetails\\\": [], \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"date\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.213Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 350.0, \\\"customObject\\\": {}}], \\\"updatedDate\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.222Z\\\"}}\",\"truncatedArrays\":null},\"source\":{\"version\":\"2.4.2.Final\",\"connector\":\"mongodb\",\"name\":\"flexi-uat-qa\",\"ts_ms\":1704887704000,\"snapshot\":\"false\",\"db\":\"flexi-gonogo\",\"sequence\":null,\"rs\":\"uat-mongodb\",\"collection\":\"loanCharges\",\"ord\":4,\"lsid\":null,\"txnNumber\":null,\"wallTime\":null},\"op\":\"u\",\"ts_ms\":1704867852326,\"transaction\":null}";
        Object valueWithNaN = "{\"before\":null,\"after\":\"{\\\"_id\\\": \\\"R-PPL23121015745\\\",\\\"institutionId\\\": \\\"1001\\\",\\\"chargesDetails\\\": [],\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 3581.0,\\\"totalCountFees\\\": 3.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 14.0,\\\"totalsGstuGstAmount\\\": 7.29,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 95.0,\\\"totalFinancedFees\\\": 0.0,\\\"userDecision\\\": false},\\\"charges\\\": {\\\"totalIGst\\\": 0.0,\\\"totalFees\\\": 0.0,\\\"totalCountFees\\\": 0.0,\\\"totalTdsAmount\\\": 0.0,\\\"totalTaxAmount\\\": 0.0,\\\"totalsGstuGstAmount\\\": 0.0},\\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0,\\\"totalGST\\\": 0.0,\\\"totalGrossPremioum\\\": 0.0,\\\"totalInitialReciept\\\": 0.0,\\\"totalDeductedfromDisb\\\": 0.0,\\\"totalFinanced\\\": 0.0}},\\\"customObject\\\": {},\\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"calBasis\\\": \\\"Amount\\\",\\\"fee\\\": NaN,\\\"taxAmount\\\": 14.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 95.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"feePercentage\\\": \\\"0\\\",\\\"tdsPercentage\\\": \\\"0\\\",\\\"grossTotalColm\\\": \\\"95.0\\\",\\\"method\\\": \\\"Forward\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 7.29,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"PROCESSING FEE\\\",\\\"calmethod\\\": \\\"Percentage\\\",\\\"calBasis\\\": \\\"Percentage\\\",\\\"fee\\\": \\\"3500.0\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feePercentage\\\": \\\"1.0\\\",\\\"calculationType\\\": \\\"Flat\\\",\\\"chargeCode\\\": \\\"500355.0\\\",\\\"chargeTypeCode\\\": \\\"logfeetype\\\",\\\"feePricingCrossDetails\\\": [],\\\"mandatory\\\": \\\"false\\\",\\\"taxApplicable\\\": \\\"yes\\\",\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"isActive\\\": \\\"true\\\",\\\"iGstRate\\\": \\\"0.0\\\",\\\"iGstAmount\\\": 0.0,\\\"sGstuGstRate\\\": \\\"9.0\\\",\\\"date\\\": {\\\"$date\\\": 1703670946527},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 0.0,\\\"customObject\\\": {}},{\\\"feeType\\\": \\\"Stamp Duty\\\",\\\"calmethod\\\": \\\"Amount\\\",\\\"taxAmount\\\": 0.0,\\\"tdsAmount\\\": 0.0,\\\"total\\\": 0.0,\\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\",\\\"method\\\": \\\"Forward\\\",\\\"chargeCode\\\": \\\"10\\\",\\\"feePricingCrossDetails\\\": [],\\\"lockInPeriod\\\": 0.0,\\\"ugstSgstAmount\\\": 0.0,\\\"date\\\": {\\\"$date\\\": 1704867852213},\\\"dsaApiTriggered\\\": false,\\\"totalAmount\\\": 350.0,\\\"customObject\\\": {}}],\\\"insurenceCatList\\\": null,\\\"updatedDate\\\": {\\\"$date\\\": 1704867852222}}\",\"updateDescription\":{\"removedFields\":null,\"updatedFields\":\"{\\\"chargesSummary\\\": {\\\"fees\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 3581.0, \\\"totalCountFees\\\": 3.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 14.0, \\\"totalsGstuGstAmount\\\": 7.29, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 95.0, \\\"totalFinancedFees\\\": 0.0, \\\"userDecision\\\": false}, \\\"charges\\\": {\\\"totalIGst\\\": 0.0, \\\"totalFees\\\": 0.0, \\\"totalCountFees\\\": 0.0, \\\"totalTdsAmount\\\": 0.0, \\\"totalTaxAmount\\\": 0.0, \\\"totalsGstuGstAmount\\\": 0.0}, \\\"insurance\\\": {\\\"totalNetPremioum\\\": 0.0, \\\"totalGST\\\": 0.0, \\\"totalGrossPremioum\\\": 0.0, \\\"totalInitialReciept\\\": 0.0, \\\"totalDeductedfromDisb\\\": 0.0, \\\"totalFinanced\\\": 0.0}}, \\\"feesDetails\\\": [{\\\"feeType\\\": \\\"apiCharges\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"calBasis\\\": \\\"Amount\\\", \\\"fee\\\": \\\"81.0\\\", \\\"taxAmount\\\": 14.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 95.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"feePercentage\\\": \\\"0\\\", \\\"tdsPercentage\\\": \\\"0\\\", \\\"grossTotalColm\\\": \\\"95.0\\\", \\\"method\\\": \\\"Forward\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 7.29, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"processingFees\\\", \\\"calmethod\\\": \\\"Percentage\\\", \\\"calBasis\\\": \\\"Percentage\\\", \\\"fee\\\": \\\"3500.0\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feePercentage\\\": \\\"1.0\\\", \\\"calculationType\\\": \\\"Flat\\\", \\\"chargeCode\\\": \\\"500355.0\\\", \\\"chargeTypeCode\\\": \\\"logfeetype\\\", \\\"feePricingCrossDetails\\\": [], \\\"mandatory\\\": \\\"false\\\", \\\"taxApplicable\\\": \\\"yes\\\", \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"isActive\\\": \\\"true\\\", \\\"iGstRate\\\": \\\"0.0\\\", \\\"iGstAmount\\\": 0.0, \\\"sGstuGstRate\\\": \\\"9.0\\\", \\\"date\\\": {\\\"$date\\\": \\\"2023-12-27T09:55:46.527Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 0.0, \\\"customObject\\\": {}}, {\\\"feeType\\\": \\\"Stamp Duty\\\", \\\"calmethod\\\": \\\"Amount\\\", \\\"taxAmount\\\": 0.0, \\\"tdsAmount\\\": 0.0, \\\"total\\\": 0.0, \\\"feeRecoveryMode\\\": \\\"Deduct from Disb\\\", \\\"method\\\": \\\"Forward\\\", \\\"chargeCode\\\": \\\"10\\\", \\\"feePricingCrossDetails\\\": [], \\\"lockInPeriod\\\": 0.0, \\\"ugstSgstAmount\\\": 0.0, \\\"date\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.213Z\\\"}, \\\"dsaApiTriggered\\\": false, \\\"totalAmount\\\": 350.0, \\\"customObject\\\": {}}], \\\"updatedDate\\\": {\\\"$date\\\": \\\"2024-01-10T06:24:12.222Z\\\"}}\",\"truncatedArrays\":null},\"source\":{\"version\":\"2.4.2.Final\",\"connector\":\"mongodb\",\"name\":\"flexi-uat-qa\",\"ts_ms\":1704887704000,\"snapshot\":\"false\",\"db\":\"flexi-gonogo\",\"sequence\":null,\"rs\":\"uat-mongodb\",\"collection\":\"loanCharges\",\"ord\":4,\"lsid\":null,\"txnNumber\":null,\"wallTime\":null},\"op\":\"u\",\"ts_ms\":1704867852326,\"transaction\":null}";
        System.out.println("2");
        assertThrows(Exception.class, () -> {
            mc_implementation.process("loanCharges", key, valueWithOid, topicObj);
        });
        System.out.println("3");


        //KeyValue<Object, Object> kv2 = mc_implementation.process("loanCharges", key, valueWithOid, topicObj);
        KeyValue<Object, Object> kv3 = mc_implementation.process("loanCharges", key, valueWithNaN, topicObj);
//        assertNull(kv2);

        String expected1 = "{\"s_api_fees\":0,\"s_application_id\":\"R-PPL23121015745\",\"s_institution_id\":1001,\"ts_ms\":1704887704000}";
        String expected2 = "{\"s_api_fees\":0,\"s_application_id\":\"R-PPL23121015745\",\"ts_ms\":1704887704000,\"s_institution_id\":1001}";
        assertTrue((kv3.value).toString().equals(expected1) || (kv3.value).toString().equals(expected2));


    }


//    @Test
//    void testProcess_NullKey() throws PipelineAuthenticationException, KStreamProcessingException {
//        mc_implementation = Mockito.spy(new MCKStreamImplementation());
//        mc_implementation.dataTypeConversionFactory = new DataTypeConversionFactory();
//
//        String topicName = "loanCharges";
//        Object nullKey = null;
//
//        doReturn(new JSONObject()).when(mc_implementation).getSourceKafkaRecord(any(), any());
//
//        doReturn(new JSONObject()).when(mc_implementation).buildDestinationValue(any(), any());
//        doNothing().when(mc_implementation).dispatchDestinationRecord(any(), any(), any());
//
//        KeyValue<Object, Object> result = mc_implementation.process(topicName, nullKey, value, topicObj);
//        assertNotNull(result);
//        assertNull(result.key);
//        //assertEquals("{}", result.value.toString());
//    }

//    @Test
//    void testProcess_InvalidJSON() {
//        mc_implementation = Mockito.spy(new MCKStreamImplementation());
//        mc_implementation.dataTypeConversionFactory = new DataTypeConversionFactory();
//
//        String topicName = "loanCharges";
//        Object key = "testKey";
//        Object invalidValue = "invalid json";
//
//        assertThrows(Exception.class, () -> {
//            mc_implementation.process(topicName, key, invalidValue, topicObj);
//        });
//    }

    @Test
    void testProcess_NullValue() {
        mc_implementation = Mockito.spy(new MCKStreamImplementation());

        String topicName = "loanCharges";
        Object key = "testKey";
        Object nullValue = null;

        assertThrows(Exception.class, () -> {
            mc_implementation.process(topicName, key, nullValue, topicObj);
        });
    }






//    @Test
//    void testProcess_EmptyValue() throws PipelineAuthenticationException, KStreamProcessingException {
//        mc_implementation = Mockito.spy(new MCKStreamImplementation());
//        mc_implementation.dataTypeConversionFactory = new DataTypeConversionFactory();
//
//        String topicName = "loanCharges";
//        Object key = "testKey";
//        Object emptyValue = "{}";
//
//        doReturn(new JSONObject()).when(mc_implementation).getSourceKafkaRecord(any(), any());
//        doReturn(new JSONObject()).when(mc_implementation).buildDestinationValue(any(), any());
//        doNothing().when(mc_implementation).dispatchDestinationRecord(any(), any(), any());
//
//        KeyValue<Object, Object> result = mc_implementation.process(topicName, key, emptyValue, topicObj);
//
//        assertNotNull(result);
//        assertEquals("testKey", result.key);
//        assertEquals("{}", result.value.toString());
//    }

    @Test
    void testProcess_ExceptionInBuildDestinationValue() throws KStreamProcessingException {
        mc_implementation = Mockito.spy(new MCKStreamImplementation());

        String topicName = "loanCharges";
        Object key = "testKey";
        Object value = "{\"valid\":\"json\"}";

        doReturn(new JSONObject()).when(mc_implementation).getSourceKafkaRecord(any(), any());
        doThrow(new RuntimeException("Test exception")).when(mc_implementation).buildDestinationValue(any(), any());

        assertThrows(RuntimeException.class, () -> {
            mc_implementation.process(topicName, key, value, topicObj);
        });
    }

//    @Test
//    void testProcess_ExceptionInDispatchDestinationRecord() throws PipelineAuthenticationException, KStreamProcessingException {
//        mc_implementation = Mockito.spy(new MCKStreamImplementation());
//        mc_implementation.dataTypeConversionFactory = new DataTypeConversionFactory();
//
//        String topicName = "loanCharges";
//        Object key = "testKey";
//        Object value = "{\"valid\":\"json\"}";
//
//        doReturn(new JSONObject()).when(mc_implementation).getSourceKafkaRecord(any(), any());
//        doReturn(new JSONObject()).when(mc_implementation).buildDestinationValue(any(), any());
//        doThrow(new RuntimeException("Test exception")).when(mc_implementation).dispatchDestinationRecord(any(), any(), any());
//
//        KeyValue<Object, Object> result = mc_implementation.process(topicName, key, value, topicObj);
//
//        assertNotNull(result);
//        assertEquals("testKey", result.key);
//        assertEquals("{}", result.value.toString());
//    }

    @Test
    void testBuildDestinationValue() {
        JSONObject testDestValue = mc_implementation.buildDestinationValue(sourceKafkaRecord, topicObj);
        System.out.println(testDestValue);
        assertEquals(81.0, testDestValue.getDouble("s_api_fees"));
        assertEquals("R-PPL23121015745", testDestValue.getString("s_application_id"));
        long test_ts_ms=1704887704000L;
        assertEquals(test_ts_ms, testDestValue.getLong("ts_ms"));
        assertEquals(1001, testDestValue.getInt("s_institution_id"));

    }

    @Test
    void testBuildDestinationValue_InvalidPath() {
        JSONObject testDestValue = mc_implementation.buildDestinationValue(sourceKafkaRecord, topicObj);
        System.out.println(testDestValue);

    }

}
