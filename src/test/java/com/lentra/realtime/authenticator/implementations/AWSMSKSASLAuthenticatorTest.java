package com.lentra.realtime.authenticator.implementations;

import com.lentra.realtime.config.store.ConfigStoreBuilder;
import com.lentra.realtime.exception.PipelineConfiguationContentNotFoundException;
import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.EndPointConstants;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.exception.PipelineAuthenticationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AWSMSKSASLAuthenticatorTest {

    private AWSMSKSASLAuthenticator authenticator;

//    @Mock
//    private AppStore.StoreContent mockStore;

    @BeforeEach
    void setUp() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException {
        authenticator = new AWSMSKSASLAuthenticator();
        AppStore.store = new ConfigStoreBuilder("src/test/resources/test_mapping.properties").build();
    }

    @Test
    void testAddSourceAuthenticationConfig() throws PipelineAuthenticationException {
        Properties kafkaSourceConfigs = new Properties();
        Map<String, String> sourceEndPointConfigs = new HashMap<>();
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SASL_MECHANISM, "SCRAM-SHA-512");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SECURITY_PROTOCOL, "SASL_SSL");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"myuser\" password=\"mypassword\";");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SASL_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        authenticator.addSourceAuthenticationConfig(kafkaSourceConfigs, sourceEndPointConfigs);

        assertEquals("SCRAM-SHA-512", kafkaSourceConfigs.getProperty("sasl.mechanism"));
        assertEquals("SASL_SSL", kafkaSourceConfigs.getProperty("security.protocol"));
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"myuser\" password=\"mypassword\";", kafkaSourceConfigs.getProperty("sasl.jaas.config"));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", kafkaSourceConfigs.getProperty("sasl.client.callback.handler.class"));
    }

    @Test
    void testAddSourceAuthenticationConfigMissingProperties() {
        Properties kafkaSourceConfigs = new Properties();
        Map<String, String> sourceEndPointConfigs = new HashMap<>();

        assertThrows(PipelineAuthenticationException.class, () -> authenticator.addSourceAuthenticationConfig(kafkaSourceConfigs, sourceEndPointConfigs));
    }

    @Test
    void testAddDestinationAuthenticationConfig() throws PipelineAuthenticationException {
        Properties kafkaDestinationConfigs = new Properties();
        Map<String, String> destinationEndPointConfigs = new HashMap<>();
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SASL_MECHANISM, "SCRAM-SHA-512");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SECURITY_PROTOCOL, "SASL_SSL");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"myuser\" password=\"mypassword\";");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SASL_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        authenticator.addDestinationAuthenticationConfig(kafkaDestinationConfigs, destinationEndPointConfigs);

        assertEquals("SCRAM-SHA-512", kafkaDestinationConfigs.getProperty("sasl.mechanism"));
        assertEquals("SASL_SSL", kafkaDestinationConfigs.getProperty("security.protocol"));
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"myuser\" password=\"mypassword\";", kafkaDestinationConfigs.getProperty("sasl.jaas.config"));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", kafkaDestinationConfigs.getProperty("sasl.client.callback.handler.class"));
    }

    @Test
    void testAddDestinationAuthenticationConfigMissingProperties() {
        Properties kafkaDestinationConfigs = new Properties();
        Map<String, String> destinationEndPointConfigs = new HashMap<>();

        assertThrows(PipelineAuthenticationException.class, () -> authenticator.addDestinationAuthenticationConfig(kafkaDestinationConfigs, destinationEndPointConfigs));
    }

    @Test
    void testAddAuthenticationConfigSource() throws PipelineAuthenticationException {
        Properties endPointProperties = new Properties();
        Map<String, String> sourceEndPointConfigs = new HashMap<>();
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SASL_MECHANISM, "SCRAM-SHA-512");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SECURITY_PROTOCOL, "SASL_SSL");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"myuser\" password=\"mypassword\";");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SASL_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        //when(mockStore.getSourceEndPointConfiguration()).thenReturn(sourceEndPointConfigs);

        authenticator.addAuthenticationConfig(endPointProperties, EndPointType.SOURCE);

        assertEquals("AWS_MSK_IAM", endPointProperties.getProperty("sasl.mechanism"));
        assertEquals("SASL_SSL", endPointProperties.getProperty("security.protocol"));
        assertEquals("software.amazon.msk.auth.iam.IAMLoginModule required;", endPointProperties.getProperty("sasl.jaas.config"));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", endPointProperties.getProperty("sasl.client.callback.handler.class"));
    }

    @Test
    void testAddAuthenticationConfigDestination() throws PipelineAuthenticationException {
        Properties endPointProperties = new Properties();
        Map<String, String> destinationEndPointConfigs = new HashMap<>();
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SASL_MECHANISM, "AWS_MSK_IAM");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SECURITY_PROTOCOL, "SASL_SSL");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"myuser\" password=\"mypassword\";");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SASL_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        //when(mockStore.getDestinationEndPointConfiguration()).thenReturn(destinationEndPointConfigs);

        authenticator.addAuthenticationConfig(endPointProperties, EndPointType.DESTINATION);

        assertEquals("AWS_MSK_IAM", endPointProperties.getProperty("sasl.mechanism"));
        assertEquals("SASL_SSL", endPointProperties.getProperty("security.protocol"));
        assertEquals("software.amazon.msk.auth.iam.IAMLoginModule required;", endPointProperties.getProperty("sasl.jaas.config"));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", endPointProperties.getProperty("sasl.client.callback.handler.class"));
    }
}