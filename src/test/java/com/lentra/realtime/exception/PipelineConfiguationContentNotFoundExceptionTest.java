package com.lentra.realtime.exception;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PipelineConfiguationContentNotFoundExceptionTest {

    @Test
    void testExceptionCreation() {
        PipelineConfiguationContentNotFoundException exception = new PipelineConfiguationContentNotFoundException();
        assertNotNull(exception);
    }

    @Test
    void testExceptionInheritance() {
        PipelineConfiguationContentNotFoundException exception = new PipelineConfiguationContentNotFoundException();
        assertTrue(exception instanceof Exception);
    }
}