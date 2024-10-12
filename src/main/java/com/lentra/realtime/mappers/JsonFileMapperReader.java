package com.lentra.realtime.mappers;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JsonFileMapperReader implements MapperReader {

    @Override
    public Optional<Map<String, String>> getPipelineConfiguration() {
        return Optional.empty();
    }
}
