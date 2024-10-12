package com.lentra.realtime.handler.datatypes;

import com.lentra.realtime.exception.DataConverterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StringUTCDateToEpochConverterTest {
    private StringUTCDateToEpochConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new StringUTCDateToEpochConverter();
    }

    @Test
    public void testConvertValidDate() throws DataConverterException {
        long expectedEpoch = 1719133596252L;
        assertEquals(expectedEpoch, converter.convert("2024-06-23T09:06:36.252365982Z"));
    }

    @Test
    public void testConvertInvalidDate() {
        assertThrows(DateTimeParseException.class, () -> {
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
