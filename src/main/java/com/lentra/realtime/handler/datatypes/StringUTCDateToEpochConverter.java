package com.lentra.realtime.handler.datatypes;

import com.lentra.realtime.exception.DataConverterException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class StringUTCDateToEpochConverter extends AbstractDataTypeConverter{
    @Override
    public Object convert(Object value) throws DataConverterException {
        if (!(value instanceof  String)){
            throw new DataConverterException(value.toString());
        }
        String dateValue = value.toString();
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(dateValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        Instant instant = offsetDateTime.toInstant();
        value = instant.toEpochMilli();
        return value;
    }
}
