package com.threathunter.greyhound.server.utils;

import com.threathunter.model.VariableMeta;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class JsonFileReader {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static List<VariableMeta> getVariableMetas(String file) throws IOException {
        List<Map<String, Object>> values = getFromResourceFile(file, List.class);

        List<VariableMeta> metas = new ArrayList<>();
        for (Map<String, Object> map : values) {
            try {
                metas.add(VariableMeta.from_json_object(map));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return metas;
    }

    public static <T> T getFromResourceFile(String file, Class<T> valueType) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        try {
            return MAPPER.readValue(is, valueType);
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }
}
