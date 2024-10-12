package com.lentra.realtime.authenticator.implementations;

import java.util.Map;
import java.util.Properties;

import com.lentra.realtime.config.store.AppStore;
import com.lentra.realtime.constants.EndPointConstants;
import com.lentra.realtime.enums.EndPointType;

//import jdk.internal.org.jline.utils.Log;

import com.lentra.realtime.exception.PipelineAuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AWSMSKSASLAuthenticator implements Authenticator
{
	private static final Logger logger=LogManager.getLogger(AWSMSKSASLAuthenticator.class);
    
    @Override
    public void addAuthenticationConfig(Properties endPointProperties, EndPointType endPointType) throws PipelineAuthenticationException {
        if (endPointType==EndPointType.SOURCE){
            addSourceAuthenticationConfig(endPointProperties, AppStore.store.getSourceEndPointConfiguration());
        }
        else if (endPointType==EndPointType.DESTINATION){
            addDestinationAuthenticationConfig(endPointProperties, AppStore.store.getDestinationEndPointConfiguration());
        }
    }
    
    public void addSourceAuthenticationConfig(Properties kafkaSourceConfigs, Map<String, String> sourceEndPointConfigs) throws PipelineAuthenticationException {
    	logger.info("Adding SASL Authentication to source Kafka");
        String saslMechanism=sourceEndPointConfigs.get(EndPointConstants.SOURCE_SASL_MECHANISM);
        if(saslMechanism==null || saslMechanism.isEmpty()){
            throw new PipelineAuthenticationException(EndPointConstants.SOURCE_SASL_MECHANISM);
        }
        kafkaSourceConfigs.put("sasl.mechanism",saslMechanism);
        String securityProtocol =sourceEndPointConfigs.get(EndPointConstants.SOURCE_SECURITY_PROTOCOL);
        if(securityProtocol==null || securityProtocol.isEmpty()){
            throw new PipelineAuthenticationException(EndPointConstants.SOURCE_SECURITY_PROTOCOL);
        }
        kafkaSourceConfigs.put("security.protocol",securityProtocol );
        String saslJaasConfig = sourceEndPointConfigs.get(EndPointConstants.SOURCE_JAAS_CONFIG);
        if(saslJaasConfig==null || saslJaasConfig.isEmpty()){
            throw new PipelineAuthenticationException(EndPointConstants.SOURCE_JAAS_CONFIG);
        }
        kafkaSourceConfigs.put("sasl.jaas.config",saslJaasConfig);
        String saslClientCallbackHandlerClass=sourceEndPointConfigs.get(EndPointConstants.SOURCE_SASL_CALLBACK_HANDLER_CLASS);
        if(saslClientCallbackHandlerClass==null || saslClientCallbackHandlerClass.isEmpty()){
            throw new PipelineAuthenticationException(EndPointConstants.SOURCE_SASL_CALLBACK_HANDLER_CLASS);
        }
        kafkaSourceConfigs.put("sasl.client.callback.handler.class",saslClientCallbackHandlerClass );
        logger.debug("Source Kafka configs for SASL Authentication {}",kafkaSourceConfigs.toString());
    }


    public void addDestinationAuthenticationConfig(Properties kafkaDestinationConfigs, Map<String, String> destinationEndPointConfigs) throws PipelineAuthenticationException {
    	logger.info("Adding SASL Authentication to destination Kafka");
        String saslMechasnism=destinationEndPointConfigs.get(EndPointConstants.DESTINATION_SASL_MECHANISM);
        if(saslMechasnism==null || saslMechasnism.isEmpty()){
            throw new PipelineAuthenticationException(EndPointConstants.DESTINATION_SASL_MECHANISM);
        }
        kafkaDestinationConfigs.put("sasl.mechanism",saslMechasnism);
        String securityProtocol = destinationEndPointConfigs.get(EndPointConstants.DESTINATION_SECURITY_PROTOCOL);
        if(securityProtocol==null || securityProtocol.isEmpty()){
            throw new PipelineAuthenticationException(EndPointConstants.DESTINATION_SECURITY_PROTOCOL);
        }
        kafkaDestinationConfigs.put("security.protocol",securityProtocol);
        String saslJaasConfig =destinationEndPointConfigs.get(EndPointConstants.DESTINATION_JAAS_CONFIG);
        if(saslJaasConfig==null || saslJaasConfig.isEmpty()){
            throw new PipelineAuthenticationException(EndPointConstants.DESTINATION_JAAS_CONFIG);
        }
        kafkaDestinationConfigs.put("sasl.jaas.config",saslJaasConfig);
        String saslClientCallbackHandlerClass=destinationEndPointConfigs.get(EndPointConstants.DESTINATION_SASL_CALLBACK_HANDLER_CLASS);
        if(saslClientCallbackHandlerClass==null || saslClientCallbackHandlerClass.isEmpty()){
            throw new PipelineAuthenticationException(EndPointConstants.DESTINATION_SASL_CALLBACK_HANDLER_CLASS);
        }
        kafkaDestinationConfigs.put("sasl.client.callback.handler.class", saslClientCallbackHandlerClass);
        logger.debug("Destination Kafka configs for SASL Authentication {}",kafkaDestinationConfigs.toString());
    }

}
