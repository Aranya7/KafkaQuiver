package com.lentra.realtime.exception;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
//import org.powermock.modules.junit5.PowerMockExtension;

import static org.junit.jupiter.api.Assertions.*;
public class PipelineImplementationExceptionTest {

    @Test
    public void testExceptionCreation() {
        String errorMessage = "Test error message";
        PipelineImplementationException exception = new PipelineImplementationException(errorMessage);
        assertNotNull(exception);
        assertTrue(exception instanceof Exception);
    }

}
