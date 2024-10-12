package com.lentra.realtime.authenticator.implementations;

import com.lentra.realtime.config.store.ConfigStoreBuilder;
import com.lentra.realtime.exception.PipelineConfiguationContentNotFoundException;
import com.lentra.realtime.exception.PipelineConfigurationNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import com.lentra.realtime.exception.PipelineAuthenticationException;

import org.mockito.junit.jupiter.MockitoExtension;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.EndPointConstants;
import com.lentra.realtime.enums.EndPointType;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@ExtendWith(MockitoExtension.class)
class SCRAMSASLAuthenticatorTest {

    private SCRAMSASLAuthenticator authenticator;

    @BeforeEach
    void setUp() throws PipelineConfigurationNotFoundException, PipelineConfiguationContentNotFoundException {
        authenticator = new SCRAMSASLAuthenticator();
        AppStore.store = new ConfigStoreBuilder("src/test/resources/test_mapping.properties").build();
    }

    @Test
    void testAddSourceAuthenticationConfig() throws PipelineAuthenticationException {
        Properties kafkaSourceConfigs = new Properties();
        Map<String, String> sourceEndPointConfigs = new HashMap<>();
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SASL_MECHANISM, "SCRAM-SHA-512");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SECURITY_PROTOCOL, "SASL_SSL");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"myuser\" password=\"mypassword\";");
        sourceEndPointConfigs.put(EndPointConstants.SOURCE_SASL_SCRAM_TRUSTSTORE_PATH, "/path/to/truststore.jks");

        authenticator.addSourceAuthenticationConfig(kafkaSourceConfigs, sourceEndPointConfigs);

        assertEquals("SCRAM-SHA-512", kafkaSourceConfigs.getProperty("sasl.mechanism"));
        assertEquals("SASL_SSL", kafkaSourceConfigs.getProperty("security.protocol"));
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"myuser\" password=\"mypassword\";", kafkaSourceConfigs.getProperty("sasl.jaas.config"));
        assertEquals("/path/to/truststore.jks", kafkaSourceConfigs.getProperty("ssl.truststore.location"));
    }

    @Test
    void testAddDestinationAuthenticationConfig() throws PipelineAuthenticationException {
        Properties kafkaDestinationConfigs = new Properties();
        Map<String, String> destinationEndPointConfigs = new HashMap<>();
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SASL_MECHANISM, "SCRAM-SHA-256");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SECURITY_PROTOCOL, "SASL_SSL");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"destuser\" password=\"destpassword\";");
        destinationEndPointConfigs.put(EndPointConstants.DESTINATION_SASL_SCRAM_TRUSTSTORE_PATH, "/path/to/dest/truststore.jks");

        authenticator.addDestinationAuthenticationConfig(kafkaDestinationConfigs, destinationEndPointConfigs);

        assertEquals("SCRAM-SHA-256", kafkaDestinationConfigs.getProperty("sasl.mechanism"));
        assertEquals("SASL_SSL", kafkaDestinationConfigs.getProperty("security.protocol"));
        assertEquals("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"destuser\" password=\"destpassword\";", kafkaDestinationConfigs.getProperty("sasl.jaas.config"));
        assertEquals("/path/to/dest/truststore.jks", kafkaDestinationConfigs.getProperty("ssl.truststore.location"));
    }

}
