package com.lentra.realtime.factories;

import java.util.HashMap;
import java.util.Map;

import com.lentra.realtime.exception.DataConverterException;
import com.lentra.realtime.handler.datatypes.*;

public class DataTypeConversionFactory
{
    private Map<String, DataTypeConverter> converters;

    public DataTypeConversionFactory()
    {
        converters = new HashMap<>();

        // connectors map
        converters.put("string-int", new StringToIntConverter());
        converters.put("string-double", new StringToDoubleConverter());
        converters.put("string-boolean", new StringToBooleanConverter());
        converters.put("string_iso_date-epoch", new StringISODateToEpochConverter());
        converters.put("string_utc_date-epoch", new StringUTCDateToEpochConverter());
        // other conversion keys and their methods to be added here
    }

    public DataTypeConverter getConverter(String sourceDataType, String destinationDataType) throws DataConverterException {
        DataTypeConverter dataTypeConverter = converters.get(sourceDataType.toLowerCase() + "-" + destinationDataType.toLowerCase());
        if(dataTypeConverter==null){
            //different exception class
            throw new DataConverterException("Converter not found for : "+sourceDataType.toLowerCase() + "-" + destinationDataType.toLowerCase());
        }
        return dataTypeConverter;
    }
}
