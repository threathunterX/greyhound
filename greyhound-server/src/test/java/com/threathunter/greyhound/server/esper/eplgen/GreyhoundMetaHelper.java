package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.utils.JsonFileReader;
import com.threathunter.greyhound.server.utils.StrategyInfoCache;
import com.threathunter.model.BaseEventMeta;
import com.threathunter.model.EventMeta;
import com.threathunter.model.EventMetaRegistry;
import com.threathunter.variable.VariableMetaBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public class GreyhoundMetaHelper {
    public static void loadEvents(String file) throws IOException {
        List<Object> obj = com.threathunter.greyhound.server.utils.JsonFileReader.getFromResourceFile(file, List.class);
        List<EventMeta> list = new ArrayList<>();
        obj.forEach(o -> {
            list.add(BaseEventMeta.from_json_object(o));
        });
        EventMetaRegistry.getInstance().updateEventMetas(list);
    }

    public static void loadVariables(String file) throws IOException {
        new VariableMetaBuilder().buildFromJson(com.threathunter.greyhound.server.utils.JsonFileReader.getFromResourceFile(file, List.class));
    }

    public static void loadStrategies(String file) throws IOException {
        StrategyInfoCache.getInstance().update(JsonFileReader.getFromResourceFile(file, List.class));
    }
}
