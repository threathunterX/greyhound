package com.threathunter.greyhound.server.esper.eplgen.strategy;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.esper.eplgen.JsonFileReader;
import com.threathunter.model.*;
import com.threathunter.variable.VariableMetaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by daisy on 17-11-20
 */
public class RealtimeVariableTest {
    @BeforeClass
    public static void globalSetup() throws IOException {
        CommonDynamicConfig.getInstance().addOverrideProperty("greyhound.variable.meta.file", "StrategyVariables.json");
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();

        List<Object> obj = JsonFileReader.getFromResourceFile("events.json", List.class);
        List<EventMeta> list = new ArrayList<>();
        obj.forEach(o -> {
            list.add(BaseEventMeta.from_json_object(o));
        });
        VariableMetaRegistry.getInstance().updateVariableMetas(new ArrayList<>());
        EventMetaRegistry.getInstance().updateEventMetas(list);
        new VariableMetaBuilder().buildFromJson(JsonFileReader.getFromResourceFile("realtime_metas.json", List.class));
    }

    @Test
    public void testVariables() {
        System.out.println(VariableMetaRegistry.getInstance().getAllVariableMetas().size());
    }
}
