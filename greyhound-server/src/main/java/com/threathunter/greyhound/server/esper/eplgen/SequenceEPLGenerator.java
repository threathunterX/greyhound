package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.model.Property;
import com.threathunter.variable.meta.SequenceVariableMeta;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public class SequenceEPLGenerator extends VariableEPLGenerator<SequenceVariableMeta> {
    @Override
    public List<EsperEPL> generateEPL(VariableMetaWrapper<SequenceVariableMeta> wrapper) {
        SequenceVariableMeta meta = wrapper.getMeta();
        StringBuilder sb = new StringBuilder();
        String firstExpression = genShiftSequenceExpression(meta.getTargetProperty(), 0);
        String secondExpression = genShiftSequenceExpression(meta.getTargetProperty(), 1);

        sb.append(genAnnotation(meta));
        sb.append(genInsertInto(meta));
        sb.append("select ").append(genGeneralProperty(meta.getApp(), meta.getName()));
        sb.append(", ").append(genGroupKeysProperty(meta.getGroupKeys()));
        sb.append(", (");
        sb.append(firstExpression);
        sb.append(" ").append(meta.getOperation()).append(" ");
        sb.append(secondExpression);
        sb.append(") as value, ");
        sb.append(firstExpression);
        sb.append(" as firstvalue, ");
        sb.append(secondExpression);
        sb.append(" as secondvalue, ");
        sb.append(genKey(meta.getGroupKeys()));
        sb.append(" from ");
        sb.append(genWindowExpression(meta.getSrcVariableMetasID().get(0).getKeys().get(1), meta.getGroupKeys()));
        sb.append(" ");
        sb.append(genWhereClause(null, "", meta.getPropertyCondition()));
        sb.append(genGroupByExpression(meta.getGroupKeys()));
        sb.append(" having ((").append(firstExpression).append(") > 0 and (").append(secondExpression).append(") > 0)");
        return Arrays.asList(new EsperEPL(genEPLName(meta), sb.toString(), true, false, getPriorityInEPL(meta)));
    }

    private String genWindowExpression(String srcName, List<Property> groupKeys) {
        String groupby = "";
        if (groupKeys == null || groupKeys.isEmpty()) {
            groupby = "key";
        } else {
            boolean first = true;
            for(Property p : groupKeys) {
                if (first) {
                    first = false;
                } else {
                    groupby += ", ";
                }

                groupby += p.getName();
            }
        }

        return String.format("`%s`.std:groupwin(%s).win:length(2) as %s", srcName, groupby, srcName);
    }

    private String genShiftSequenceExpression(Property targetProperty, int shift) {
        return String.format("last(%s.%s, %d)", targetProperty.getIdentifier().getKeys().get(1), targetProperty.getName(), shift);
    }

}
