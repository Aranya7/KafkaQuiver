package com.lentra.realtime.handler.datatypes;

import com.lentra.realtime.exception.DataConverterException;

public class StringToDoubleConverter extends AbstractDataTypeConverter
{	
    @Override
    public Object convert(Object value) throws DataConverterException {
        if(!(value instanceof String)){
            throw new DataConverterException(value.toString());
        }
        return Double.parseDouble(value.toString());

    }
}
