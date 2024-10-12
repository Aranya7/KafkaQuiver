package com.lentra.realtime.handler.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lentra.realtime.exception.DataConverterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StringISODateToEpochConverterTest {

    private StringISODateToEpochConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new StringISODateToEpochConverter();
    }

    @Test
    public void testConvertValidDate() throws DataConverterException {
        long expectedEpoch = 1719100800000L;
        assertEquals(expectedEpoch, converter.convert("2024-06-23"));
    }

    @Test
    public void testConvertInvalidDate() {
        assertThrows(DataConverterException.class, () -> {
            converter.convert("invalid date");
        });
    }

    @Test
    public void testConvertNull() {
        assertThrows(NullPointerException.class, () -> {
            converter.convert(null);
        });
    }
}
