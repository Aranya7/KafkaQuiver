package com.lentra.realtime.authenticator.implementations;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.EndPointConstants;
import com.lentra.realtime.enums.EndPointType;

import java.util.Map;
import java.util.Properties;

public class SCRAMSASLAuthenticator implements Authenticator {
    @Override
    public void addAuthenticationConfig(Properties endPointProperties, EndPointType endPointType) {
        if (endPointType==EndPointType.SOURCE){
            addSourceAuthenticationConfig(endPointProperties, AppStore.store.getSourceEndPointConfiguration());
        }
        else if (endPointType==EndPointType.DESTINATION){
            addDestinationAuthenticationConfig(endPointProperties, AppStore.store.getDestinationEndPointTopicConfiguration());
        }
    }

    public void addSourceAuthenticationConfig(Properties kafkaSourceConfigs, Map<String, String> sourceEndPointConfigs) {
        kafkaSourceConfigs.put("sasl.mechanism", sourceEndPointConfigs.get(EndPointConstants.SOURCE_SASL_MECHANISM));
        kafkaSourceConfigs.put("security.protocol", sourceEndPointConfigs.get(EndPointConstants.SOURCE_SECURITY_PROTOCOL));
        kafkaSourceConfigs.put("sasl.jaas.config", sourceEndPointConfigs.get(EndPointConstants.SOURCE_JAAS_CONFIG));
        kafkaSourceConfigs.put("ssl.truststore.location", sourceEndPointConfigs.get(EndPointConstants.SOURCE_SASL_SCRAM_TRUSTSTORE_PATH));
    }

    public void addDestinationAuthenticationConfig(Properties kafkaDestinationConfigs, Map<String, String> destinationEndPointConfigs) {
        kafkaDestinationConfigs.put("sasl.mechanism", destinationEndPointConfigs.get(EndPointConstants.DESTINATION_SASL_MECHANISM));
        kafkaDestinationConfigs.put("security.protocol", destinationEndPointConfigs.get(EndPointConstants.DESTINATION_SECURITY_PROTOCOL));
        kafkaDestinationConfigs.put("sasl.jaas.config", destinationEndPointConfigs.get(EndPointConstants.DESTINATION_JAAS_CONFIG));
        kafkaDestinationConfigs.put("ssl.truststore.location", destinationEndPointConfigs.get(EndPointConstants.DESTINATION_SASL_SCRAM_TRUSTSTORE_PATH));
    }
}
