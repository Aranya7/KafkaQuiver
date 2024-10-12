package com.lentra.realtime.mappers;

import java.util.HashMap;

public abstract class AbstractMapperReader implements MapperReader
{	
	protected HashMap<String, String> cacheMapping = new HashMap<>();
}
