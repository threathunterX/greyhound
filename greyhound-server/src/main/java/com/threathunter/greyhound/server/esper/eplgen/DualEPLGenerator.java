package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.Identifier;
import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.model.Property;
import com.threathunter.variable.meta.DualVariableMeta;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public class DualEPLGenerator extends VariableEPLGenerator<DualVariableMeta> {
    @Override
    public List<EsperEPL> generateEPL(VariableMetaWrapper<DualVariableMeta> wrapper) {
        DualVariableMeta meta = wrapper.getMeta();
        StringBuilder sb = new StringBuilder();
        sb.append(genAnnotation(meta));
        sb.append(genInsertInto(meta));
        sb.append("select ").append(genGeneralProperty(meta.getApp(), meta.getName()));
        sb.append(", ").append(genGroupKeysProperty(meta.getGroupKeys()));
        sb.append(", ").append(genKey(meta.getGroupKeys(), meta.getSrcVariableMetasID()));
        sb.append(", ").append(genValueExpression(meta.getFirstProperty(), meta.getSecondProperty(), meta.getOperation()));
        sb.append(" from ").append(genJoinClause(meta.getFirstId(), meta.isFirstMayNull(),
                meta.getSecondId(), meta.isSecondMayNull(), meta.getOperation()));
        EsperEPL epl = new EsperEPL(genEPLName(meta), sb.toString(), true, false, getPriorityInEPL(meta));
        return Arrays.asList(epl);
    }

    private String genValueExpression(Property first, Property second, String operation) {
        StringBuilder sb = new StringBuilder();
        String firstValueExp = "coalesce(" + first.getIdentifier().getKeys().get(1) + "." + first.getName() + ", 0)";
        String secondValueExp = "coalesce(" + second.getIdentifier().getKeys().get(1) + "." + second.getName() + ", 0)";
        sb.append("(");
        sb.append(getOperationExpression(operation, firstValueExp, secondValueExp));
        sb.append(") as value");

        return sb.toString();
    }

    private String genJoinClause(Identifier first, boolean firstNullable, Identifier second, boolean secondNullable, String operator) {
        StringBuilder sb = new StringBuilder();
//        if (firstNullable && secondNullable) {
//            sb.append(" full outer join ");
//        } else if (firstNullable) {
//            sb.append(" right outer join ");
//        } else if (secondNullable) {
//            sb.append(" left outer join ");
//        } else {
//            sb.append(" inner join ");
//        }
        if (operator.equals("-") || operator.equals("/")) {
            sb.append("`").append(EPLGenUtil.genVariableName(first)).append("`").append(" as ").append(first.getKeys().get(1));
            sb.append(" unidirectional ");
            sb.append(" join ");
        } else {
            // for (+)
            sb.append("`").append(EPLGenUtil.genVariableName(first)).append("`.std:lastevent() as ").append(first
                    .getKeys().get(1));
            sb.append(" full outer join ");

        }
        sb.append("`").append(EPLGenUtil.genVariableName(second)).append("`.std:lastevent() as ").
                append(second.getKeys().get(1));
        sb.append(" on ");
        sb.append(first.getKeys().get(1)).append(".key = ").append(second.getKeys().get(1)).append(".key");

        return sb.toString();
    }

    private String getOperationExpression(String operation, String firstValueExp, String secondValueExp) {
        if (operation.equals("/")) {
            return String.format("OperationExt.doubleDivide(%s, %s)", firstValueExp, secondValueExp);
        }

        return String.format("%s %s %s", firstValueExp, operation, secondValueExp);
    }
}
