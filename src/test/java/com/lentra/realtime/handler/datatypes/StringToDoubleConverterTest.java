package com.lentra.realtime.handler.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lentra.realtime.exception.DataConverterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StringToDoubleConverterTest {

    private StringToDoubleConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new StringToDoubleConverter();
    }

    @Test
    public void testConvertValidString() throws DataConverterException {
        assertEquals(123.45, converter.convert("123.45"));
    }

    @Test
    public void testConvertInvalidString() {
        assertThrows(NumberFormatException.class, () -> {
            converter.convert("abc");
        });
    }

    @Test
    public void testConvertNull() {
        assertThrows(NullPointerException.class, () -> {
            converter.convert(null);
        });
    }
}
