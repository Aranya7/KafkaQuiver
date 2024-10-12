package com.lentra.realtime.handler.datatypes;

import com.lentra.realtime.exception.DataConverterException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class StringISODateToEpochConverter extends AbstractDataTypeConverter{
    @Override
    public Object convert(Object value) throws DataConverterException {
        String dateValue = value.toString();
        if (!(value instanceof  String) || !(dateValue.matches("^\\d{4}-\\d{2}-\\d{2}$"))) {
            throw new DataConverterException(value.toString());
        }
        else{
            LocalDate localDate = LocalDate.parse(dateValue, DateTimeFormatter.ISO_DATE);
            Instant instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
            value = instant.toEpochMilli();
            return value;
        }

    }
}
