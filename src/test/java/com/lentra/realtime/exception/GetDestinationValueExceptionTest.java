package com.lentra.realtime.exception;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.json.JSONObject;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import static org.junit.jupiter.api.Assertions.*;

class GetDestinationValueExceptionTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @BeforeEach
    void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    void restoreStreams() {
        System.setOut(originalOut);
    }

    @Test
    void testConstructorWithString() {
        String errorMessage = "Test error message";
        GetDestinationValueException exception = new GetDestinationValueException(errorMessage);
        assertNotNull(exception);
    }

    @Test
    void testConstructorWithJSONObject() {
        JSONObject jsonObject = new JSONObject();
        GetDestinationValueException exception = new GetDestinationValueException(jsonObject);
        assertNotNull(exception);
    }

}

