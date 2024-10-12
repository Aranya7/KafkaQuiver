package com.lentra.realtime.factories;

import static com.lentra.realtime.constants.PipelineConstants.*;

import java.util.Map;
import java.util.Optional;

import com.lentra.realtime.authenticator.implementations.AWSMSKSASLAuthenticator;
import com.lentra.realtime.authenticator.implementations.Authenticator;
import com.lentra.realtime.authenticator.implementations.DefaultAuthenticator;
import com.lentra.realtime.authenticator.implementations.SCRAMSASLAuthenticator;
import com.lentra.realtime.constants.AuthenticatorConstants;

public abstract class AuthenticatorFactory {

    public static Authenticator getAuthenticator(boolean isAuthenticationEnabled, String protocol, String mechanism) {
        if(!isAuthenticationEnabled){
            return new DefaultAuthenticator();
        }

        if(protocol.equalsIgnoreCase(AuthenticatorConstants.AUTHENTICATOR_SASL_SSL_SECURITY_PROTOCOL) && mechanism.equalsIgnoreCase(AuthenticatorConstants.AUTHENTICATOR_MSK_SASL_MECHANISM)){
            return new AWSMSKSASLAuthenticator();
        }
        else if(protocol.equalsIgnoreCase(AuthenticatorConstants.AUTHENTICATOR_SASL_SSL_SECURITY_PROTOCOL) && mechanism.equalsIgnoreCase(AuthenticatorConstants.AUTHENTICATOR_SCRAM_SASL_MECHANISM)){
            return new SCRAMSASLAuthenticator();
        }

        return null;
    }
}
