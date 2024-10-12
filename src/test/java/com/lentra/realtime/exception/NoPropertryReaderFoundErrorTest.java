package com.lentra.realtime.exception;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NoPropertryReaderFoundErrorTest {

    @Test
    void testConstructorWithMessage() {
        String errorMessage = "Test error message";
        NoPropertryReaderFoundError error = new NoPropertryReaderFoundError(errorMessage);
        assertNotNull(error);
        assertEquals(errorMessage, error.getMessage());
    }

    @Test
    void testExceptionInheritance() {
        NoPropertryReaderFoundError error = new NoPropertryReaderFoundError("Test");
        assertTrue(error instanceof RuntimeException);
    }
}