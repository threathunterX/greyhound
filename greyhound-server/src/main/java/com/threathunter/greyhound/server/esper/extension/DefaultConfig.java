package com.threathunter.greyhound.server.esper.extension;

import com.threathunter.common.Identifier;
import com.threathunter.model.BaseEventMeta;
import com.threathunter.model.EventMeta;
import com.threathunter.model.Property;

import java.util.Arrays;

import static com.threathunter.common.NamedType.LONG;
import static com.threathunter.common.NamedType.STRING;

/**
 * Build default event and config.
 *
 * @author Wen Lu
 */
public class DefaultConfig {

    public static void buildDefaultEvent() {
        buildVariableQueryEvent();
        buildVariableKeyTopQueryEvent();
    }

//    public static void buildDefaultVariable() {
//        buildVariableQueryVar();
//        buildVariableKeyTopQueryVar();
//    }

    private static EventMeta buildVariableQueryEvent() {
        String app = "__all__";
        String name = "_variablequery";
        Identifier id = Identifier.fromKeys(app, name);
        BaseEventMeta.BaseEventMetaBuilder builder = BaseEventMeta.builder();
        builder.setApp(app)
                .setName(name)
                .setDerived(false)
                .setProperties(Arrays.asList(
                        new Property(id, "query", STRING),
                        new Property(id, "requestid", LONG)));

        return builder.build();
    }

    private static EventMeta buildVariableKeyTopQueryEvent() {
        String app = "__all__";
        String name = "_variablekeytopquery";
        Identifier id = Identifier.fromKeys(app, name);
        BaseEventMeta.BaseEventMetaBuilder builder = BaseEventMeta.builder();
        builder.setApp(app)
                .setName(name)
                .setDerived(false)
                .setProperties(Arrays.asList(
                        new Property(id, "variablename", STRING),
                        new Property(id, "keypattern", STRING),
                        new Property(id, "requestid", LONG)));

        return builder.build();
    }

//    private static VariableMeta buildVariableQueryVar() {
//        EventVariableMeta.EventVariableMetaBuilder builder = EventVariableMeta.EventVariableMetaBuilder.newBuilder();
//        return builder.setSrcEventID(Identifier.fromKeys("__all__", "_variablequery")).build();
//    }
//
//    private static VariableMeta buildVariableKeyTopQueryVar() {
//        EventVariableMeta.EventVariableMetaBuilder builder = EventVariableMeta.EventVariableMetaBuilder.newBuilder();
//        return builder.setSrcEventID(Identifier.fromKeys("__all__", "_variablekeytopquery")).build();
//    }
}
