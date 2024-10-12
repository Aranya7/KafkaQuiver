package com.lentra.realtime.handler.datatypes;

import com.lentra.realtime.exception.DataConverterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StringToIntConverterTest {
    private StringToIntConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new StringToIntConverter();
    }

    @Test
    public void testConvertValidString() throws DataConverterException {
        assertEquals(123, converter.convert("123"));
    }

    @Test
    public void testConvertInvalidString() throws DataConverterException {
        assertThrows(NumberFormatException.class, () -> {
            converter.convert("abc");
        });
    }

    @Test
    public void testConvertNull() throws DataConverterException {
        assertThrows(NullPointerException.class, () -> {
            converter.convert(null);
        });
    }
}
