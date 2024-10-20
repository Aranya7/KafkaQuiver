package com.lentra.realtime.exception;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

class PipelineConfigurationNotFoundExceptionTest {

    @Test
    void testExceptionCreation() {
        PipelineConfigurationNotFoundException exception = new PipelineConfigurationNotFoundException();
        assertNotNull(exception);
    }

    @Test
    void testExceptionInheritance() {
        PipelineConfigurationNotFoundException exception = new PipelineConfigurationNotFoundException();
        assertTrue(exception instanceof IOException);
    }

//    @Test
//    void testHandleException() {
//        PipelineConfigurationNotFoundException exception = new PipelineConfigurationNotFoundException();
//        exception.handleException();
//        // Since handleException() only logs an error, we can't easily test its behavior without mocking the logger
//        // For now, we'll just ensure it doesn't throw any exceptions
//        assertDoesNotThrow(() -> exception.handleException());
//    }
}