package com.lentra.realtime.constants;

public class EndPointConstants 
{
	private EndPointConstants() {};
	public static final String SOURCE_BOOTSTRAP_URL="source.bootstrap.url";
	public static final String DESTINATION_BOOTSTRAP_URL="destination.bootstrap.url";
	public static final String SOURCE_AUTHENTICATION_ENABLED="source.authentication.enabled";
	public static final String DESTINATION_AUTHENTICATION_ENABLED="destination.authentication.enabled";
	
	public static final String SOURCE_SECURITY_PROTOCOL="source.security.protocol";
	public static final String DESTINATION_SECURITY_PROTOCOL="destination.security.protocol";
	
	public static final String SOURCE_SASL_MECHANISM="source.sasl.mechanism";
	public static final String DESTINATION_SASL_MECHANISM="destination.sasl.mechanism";

	public static final String SOURCE_JAAS_CONFIG="source.sasl.jaas.config";
	public static final String DESTINATION_JAAS_CONFIG="destination.sasl.jaas.config";

	public static final String SOURCE_SASL_CALLBACK_HANDLER_CLASS="source.sasl.client.callback.handler.class";
	public static final String SOURCE_SASL_SCRAM_TRUSTSTORE_PATH="source.sasl.truststore.path";
	public static final String DESTINATION_SASL_CALLBACK_HANDLER_CLASS="destination.sasl.client.callback.handler.class";
	public static final String DESTINATION_SASL_SCRAM_TRUSTSTORE_PATH="destination.sasl.truststore.path";
	
	public static final String SOURCE_PREFIX="source.";
	public static final String DESTINATION_PREFIX="destination.";
}
