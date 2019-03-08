package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.NamedType;
import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.variable.meta.EventVariableMeta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Generate epl for {@link com.threathunter.model.VariableMeta} of EVENT type.
 *
 */
public class EventEPLGenerator extends VariableEPLGenerator<EventVariableMeta> {

    @Override
    public List<EsperEPL> generateEPL(VariableMetaWrapper<EventVariableMeta> wrapper) {
        EventVariableMeta meta = wrapper.getMeta();
        StringBuilder sb = new StringBuilder();
        sb.append("create schema ");
        sb.append(meta.getName());
        sb.append(" as (");

        boolean first = true;
        for (Map.Entry<String, NamedType> entry : meta.getDataSchema().entrySet()) {
            if (first) {
                first = false;
                sb.append("`").append(entry.getKey()).append("` ").append(entry.getValue().getCode());
            } else {
                sb.append(", `").append(entry.getKey()).append("` ").append(entry.getValue().getCode());
            }
        }
        sb.append(")");
             EsperEPL epl = new EsperEPL(genEPLName(meta), sb.toString(), true, false, getPriorityInEPL(meta));
        return Arrays.asList(epl);
    }
}
