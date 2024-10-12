package com.lentra.realtime.handler.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lentra.realtime.exception.DataConverterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StringToBooleanConverterTest {
    private StringToBooleanConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new StringToBooleanConverter();
    }

    @Test
    public void testConvertTrue() throws DataConverterException {
        assertEquals(true, converter.convert("true"));
    }

    @Test
    public void testConvertFalse() throws DataConverterException {
        assertEquals(false, converter.convert("false"));
    }

//    @Test
//    public void testConvertInvalidString() {
//        assertThrows(IllegalArgumentException.class, () -> {
//            converter.convert("not a boolean");
//        });
//    }

    @Test
    public void testConvertNull() {
        assertThrows(NullPointerException.class, () -> {
            converter.convert(null);
        });
    }
}
