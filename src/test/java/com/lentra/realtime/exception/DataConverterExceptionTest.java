package com.lentra.realtime.exception;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import static org.junit.jupiter.api.Assertions.*;

class DataConverterExceptionTest {

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
        DataConverterException exception = new DataConverterException(errorMessage);
        assertNotNull(exception);
    }

//    @Test
//    void testHandleException() {
//        String errorMessage = "Test error message";
//        DataConverterException exception = new DataConverterException(errorMessage);
//
//        try {
//            exception.handleException();
//            fail("Expected SecurityException was not thrown");
//        } catch (SecurityException se) {
//            // System.exit(0) call in handleException() method will throw SecurityException in test environment
//            assertTrue(se.getMessage().contains("exit"));
//        }
//    }
//
//    @Test
//    void testHandelDataConverterException() {
//        DataConverterException exception = new DataConverterException("Test error");
//
//        try {
//            exception.handelDataConverterException();
//            fail("Expected SecurityException was not thrown");
//        } catch (SecurityException se) {
//            // System.exit(0) call in handelDataConverterException() method will throw SecurityException in test environment
//            assertTrue(se.getMessage().contains("exit"));
//        }
//    }

    @Test
    void testExceptionInheritance() {
        DataConverterException exception = new DataConverterException("Test error");
        assertTrue(exception instanceof Exception);
    }
}