package com.lentra.realtime.entity.endpoint;

import com.lentra.realtime.authenticator.implementations.Authenticator;
import com.lentra.realtime.constants.EndPointConstants;
import com.lentra.realtime.enums.EndPointType;
import com.lentra.realtime.exception.PipelineInitializationException;
import com.lentra.realtime.factories.AuthenticatorFactory;

import static com.lentra.realtime.constants.PipelineConstants.*;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DestinationEndPoint extends AbstractEndPoint
{
    public DestinationEndPoint()
    {
        super(EndPointType.DESTINATION);
    }

    @Override
    public String getBootStrapURL(Map<String,String> destinationConfigMap)
    {
    	return destinationConfigMap.get(EndPointConstants.DESTINATION_BOOTSTRAP_URL);
    }
    
    @Override
    public boolean checkAuthenticationEnabled(Map<String,String> destinationConfigMap)
    {
    	return Boolean.parseBoolean(destinationConfigMap.get(EndPointConstants.DESTINATION_AUTHENTICATION_ENABLED)) ;
    }
    
    @Override
    public Authenticator getAuthenticator(Map<String,String> destinationConfigMap)
    {
    	return AuthenticatorFactory.getAuthenticator(isAuthenticationEnabled,
    			destinationConfigMap.get(EndPointConstants.DESTINATION_SECURITY_PROTOCOL),
                destinationConfigMap.get(EndPointConstants.DESTINATION_SASL_MECHANISM));
    }
    private void initializeBootstrapURL( Map<String,String> destinationConfigMap) throws PipelineInitializationException{
        this.bootstrapURL= getBootStrapURL(destinationConfigMap);
        if(bootstrapURL==null){
            throw new PipelineInitializationException(EndPointConstants.DESTINATION_BOOTSTRAP_URL);
        }
    }
    private void initializeIsAuthenticationEnabled( Map<String,String> destinationConfigMap) throws PipelineInitializationException{
        this.isAuthenticationEnabled=checkAuthenticationEnabled(destinationConfigMap);
    }
    private void initializeAuthenticator(Map<String,String> destinationConfigMap) throws PipelineInitializationException{
        this.authenticator = getAuthenticator(destinationConfigMap);
        if(authenticator==null){
            throw new PipelineInitializationException(EndPointConstants.DESTINATION_SECURITY_PROTOCOL+" or "+EndPointConstants.DESTINATION_SASL_MECHANISM);
        }
    }
    
    @Override
    public void initializeEndPoint() throws PipelineInitializationException {
    	 Map<String,String> destinationConfigMap = getEndPointConfiguration();

        initializeBootstrapURL(destinationConfigMap);
        initializeIsAuthenticationEnabled(destinationConfigMap);
        initializeAuthenticator(destinationConfigMap);

//        if (isAuthenticationEnabled){
//            authenticator.addAuthenticationConfig(this.endPointType);
//        }

    }

	@Override
	public boolean initializeEndPointTopics() {
		// TODO Auto-generated method stub
		return false;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Destination BootStrapURL = " + bootstrapURL + "\n");
		sb.append("Destination Authentication Enabled = " + isAuthenticationEnabled + "\n");
		sb.append("Destination Authentication Type = " + typeOfAuthentication + "\n");
		sb.append("Destination Authentication Protocol = " + protocol + "\n");
		sb.append("Destination EndPoint Type = " + endPointType + "\n");
		return sb.toString();
	}
}
