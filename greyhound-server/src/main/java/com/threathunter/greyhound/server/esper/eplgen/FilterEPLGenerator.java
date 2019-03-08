package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.Identifier;
import com.threathunter.common.NamedType;
import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.variable.meta.FilterVariableMeta;

import java.util.Arrays;
import java.util.List;

/**
 * Created by daisy on 17/8/24.
 */
public class FilterEPLGenerator extends VariableEPLGenerator<FilterVariableMeta> {

    @Override
    public List<EsperEPL> generateEPL(VariableMetaWrapper<FilterVariableMeta> wrapper) {
        FilterVariableMeta meta = wrapper.getMeta();
        StringBuilder sb = new StringBuilder();
        sb.append(genAnnotation(meta));
        sb.append(genInsertInto(meta));
        sb.append("select ").append(genGeneralProperty(meta.getApp(), meta.getName()));
        sb.append(", ").append(genPropertyMappings(meta.getPropertyMappings()));
        sb.append(", ");
        sb.append(genDirectValue(meta.getSrcVariableMetasID().get(0), NamedType.fromCode(meta.getValueType())));
        sb.append(", '' as key");
        sb.append(" from `").append(EPLGenUtil.genVariableName(meta.getSrcVariableMetasID().get(0))).append("` ");
        sb.append(genWhereClause(null, "", meta.getPropertyCondition()));
        EsperEPL epl = new EsperEPL(genEPLName(meta), sb.toString(), true, false, getPriorityInEPL(meta));
        return Arrays.asList(epl);
    }

    private String genDirectValue(Identifier srcId, NamedType valueType) {
        StringBuilder builder = new StringBuilder();
        if (valueType.equals(NamedType.DOUBLE)) {
            builder.append("coalesce(");
        }

        List<String> keysOfId = srcId.getKeys();
        String lastKey = keysOfId.get(keysOfId.size()-1);
        builder.append(lastKey).append(".");
        builder.append("value");
        if (valueType.equals(NamedType.DOUBLE)) {
            builder.append(", 0)");
        }
        builder.append(" as value");

        return builder.toString();
    }
}
