package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.greyhound.server.esper.eplgen.property.PropertyReductionEPLGenerator;
import com.threathunter.variable.WindowType;
import com.threathunter.variable.meta.AggregateVariableMeta;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public class AggregationEPLGenerator extends VariableEPLGenerator<AggregateVariableMeta> {

    @Override
    public List<EsperEPL> generateEPL(VariableMetaWrapper<AggregateVariableMeta> wrapper) {
        AggregateVariableMeta meta = wrapper.getMeta();
        StringBuilder sb = new StringBuilder();
        sb.append(genAnnotation(meta));
        sb.append(genInsertInto(meta));
        sb.append("select ").append(genGeneralProperty(meta.getApp(), meta.getName()));
        sb.append(", ").append(genGroupKeysProperty(meta.getGroupKeys()));
        sb.append(", ");
        sb.append(PropertyReductionEPLGenerator.genExpressionFromReduction(meta.getPropertyReduction()));
        sb.append(", ");
        sb.append(genKey(meta.getGroupKeys()));
        sb.append(" from `").append(EPLGenUtil.genVariableName(meta.getSrcVariableMetasID().get(0))).append("`").append(
                genWindowExpression(getWindowType(meta.getAggregateType().toString()), meta.getTtl()));
        if (meta.getPropertyCondition() != null) {
            sb.append(genWhereClause(null, "", meta.getPropertyCondition()));
        }
        sb.append(" ");
        sb.append(genGroupByExpression(meta.getGroupKeys()));
        EsperEPL epl = new EsperEPL(genEPLName(meta), sb.toString(), true, false, getPriorityInEPL(meta));
        return Arrays.asList(epl);
    }

    private WindowType getWindowType(String type) {
        if (type.equals("realtime")) {
            return WindowType.TIME_SLIDING;
        }
        return WindowType.Length_SLOT;
    }

    private String genWindowExpression(WindowType windowType, long windowLength) {
        if (windowType == WindowType.TIME_SLIDING) {
            return String.format(".win:time(%d seconds)", windowLength);
        }
        if (windowType == WindowType.TIME_SLOT) {
            return String.format(".win:time_batch(%d seconds)", windowLength);
        }
        if (windowType == WindowType.Length_SLIDING) {
            return String.format(".win:length(%d)", windowLength);
        }
        if (windowType == WindowType.Length_SLOT) {
            return String.format(".win:length_batch(%d)", windowLength);
        }
        return "";
    }
}
