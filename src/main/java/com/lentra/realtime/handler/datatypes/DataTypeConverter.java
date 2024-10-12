package com.lentra.realtime.handler.datatypes;

import com.lentra.realtime.exception.DataConverterException;
import com.lentra.realtime.exception.KStreamProcessingException;

public interface DataTypeConverter
{
    public Object convert(Object value) throws KStreamProcessingException, DataConverterException;
}
