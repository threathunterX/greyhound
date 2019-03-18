package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.Identifier;
import com.threathunter.common.NamedType;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.model.*;
import com.threathunter.variable.VariableMetaBuilder;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.threathunter.greyhound.server.esper.eplgen.GreyhoundMetaHelper.loadEvents;

/**
 * 
 */
public class VariableMetaTestHelper {
    private static Map<String, VariableMeta> metas;
    static {
        try {
            loadEvents("events.json");
        } catch (IOException e) {
            e.printStackTrace();
        }
        metas = new HashMap<>();
        try {
            VariableMetaRegistry.getInstance().updateVariableMetas(new ArrayList<>());
            String metaFile = CommonDynamicConfig.getInstance().getString("greyhound.variable.meta.file", "metas.json");
            List<VariableMeta> list = new VariableMetaBuilder().buildFromJson(JsonFileReader.getValuesFromFile(metaFile, JsonFileReader.ClassType.LIST));
            list.forEach(meta -> metas.put(meta.getName(), meta));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static VariableMeta getMetaByName(String name) {
        return metas.get(name);
    }

    public static List<VariableMeta> getAll() {
        List<VariableMeta> list = new ArrayList<>(metas.values());
        list.sort(Comparator.comparingInt(VariableMeta::getPriority));

        return list;
    }

    public static List<VariableMetaWrapper> getAllWrapper() {
        List<VariableMetaWrapper> list = new ArrayList<>();
        metas.values().forEach(m -> list.add(new VariableMetaWrapper(m, null)));
        list.sort(Comparator.comparingInt((wrapper) -> wrapper.getMeta().getPriority()));

        return list;
    }

    static EventMeta createEventMeta(String name) {
        Identifier id = Identifier.fromKeys("nebula", name);
        List<Property> props = new ArrayList();
        props.add(Property.buildStringProperty(id, "c_ip"));
        props.add(Property.buildStringProperty(id, "uid"));
        props.add(Property.buildStringProperty(id, "did"));
        props.add(Property.buildStringProperty(id, "method"));
        props.add(Property.buildStringProperty(id, "s_type"));
        props.add(Property.buildStringProperty(id, "result"));
        props.add(Property.buildLongProperty(id, "c_bytes"));
        props.add(Property.buildLongProperty(id, "s_bytes"));
        // {param=1000, srcProperty={identifier=[nebula, HTTP_DYNAMIC], name=s_bytes, type=long}, type=long>}

        Set<String> propSet = new HashSet<>();
        propSet.add("c_ip");
        propSet.add("uid");
        propSet.add("did");
        propSet.add("c_bytes");
        propSet.add("s_bytes");
        propSet.add("method");
        propSet.add("s_type");
        return new EventMeta() {
            @Override
            public String getApp() {
                return "nebula";
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public boolean isDerived() {
                return false;
            }

            @Override
            public Identifier getSrcVariableID() {
                return null;
            }

            @Override
            public List<Property> getProperties() {
                return props;
            }

            @Override
            public boolean hasProperty(Property property) {
                return props.contains(property.getName());
            }

            @Override
            public Map<String, NamedType> getDataSchema() {
                return null;
            }

            @Override
            public long expireAt() {
                return 0;
            }

            @Override
            public void active() {

            }

            @Override
            public void deactive() {

            }

            @Override
            public String getRemark() {
                return "";
            }
        };
    }
}
