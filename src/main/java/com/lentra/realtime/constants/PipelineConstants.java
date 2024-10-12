package com.lentra.realtime.constants;

public class PipelineConstants {
	
	private PipelineConstants() {};
	
	// PIPELINE CONSTANTS 
	public static final String PIPELINE_FEATURE_LIST="pipeline.features.list";
	public static final String PIPELINE_INSITUTION_MAPPING_KEY="pipeline.institution.id";
	public static final String PIPELINE_APP_NAME_MAPPING_KEY="pipeline.appName";
	public static final String PIPELINE_CONSUMER_START_POINT_KEY="pipeline.consumer.start.point";
	public static final String PIPELINE_PREFIX = "pipeline.";
	public static final Object PIPELINE_STREAMING_MEDIUM = "pipeline.streaming.medium";
	public static final Object PIPELINE_IMPLEMENTATION_CLIENT = "pipeline.implementation.client";
	
	
	
	

    
    
    public static String destination_bootstrap_url="destination.bootstrap.url";
   
    public static String destination_authentication_enabled="destination.authentication.enabled";


 
   
    public static String source_jaas_config="source.sasl.jaas.config";
    public static String source_callback_handler_class="source.sasl.client.callback.handler.class";
    public static String source_topics_list="source.topic.list";
    public static String source_topic_prefix="source.topic";
    
    public static String destination_security_protocol="destination.security.protocol";
    public static String destination_sasl_mechanism="AWS_MSK_IAM";
    public static String destination_jaas_config="destination.sasl.jaas.config";
    public static String destination_callback_handler_class="destination.sasl.client.callback.handler.class";
    public static String destination_topics_list="destination.topic.list";
    
   
    
    public static String topic_mapping_fields="mapping.field";


}
