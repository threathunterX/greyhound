package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.Identifier;
import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.model.Property;
import com.threathunter.variable.meta.CollectorVariableMeta;

import java.util.Arrays;
import java.util.List;

/**
 * Created by daisy on 17/8/25.
 */
public class CollectorEPLGenerator extends VariableEPLGenerator<CollectorVariableMeta> {

    @Override
    public List<EsperEPL> generateEPL(VariableMetaWrapper<CollectorVariableMeta> wrapper) {
        CollectorVariableMeta meta = wrapper.getMeta();
        StringBuilder sb = new StringBuilder();
        sb.append(genAnnotation(meta));
        sb.append(genInsertInto(meta));
        sb.append("select ").append(genGeneralProperty(meta.getApp(), meta.getName()));
        sb.append(", '").append(meta.getStrategyName()).append("' as strategyName");

        for (Identifier id : meta.getSrcVariableMetasID()) {
            if (!wrapper.isInBatchQueryIds(id)) {
                String name = id.getKeys().get(1);
                sb.append(", ").append(name);
                if (meta.isOnlyValueNeeded()) {
                    sb.append(".value");
                } else {
                    sb.append(".*");
                }
                sb.append(" as ").append(name);
            }
        }

        String triggerKeysExp = genTriggerKeysString(meta.getTrigger(), meta.getGroupKeys());
        String triggerKeysExpression = triggerKeysExp;
        if (meta.getGroupKeys().size() > 1) {
            triggerKeysExpression = wrapTriggerKeys(triggerKeysExp);
        }

        String triggerName = meta.getTrigger().getKeys().get(1);
        sb.append(", ").append(triggerName).append(".* as triggerevent");
        sb.append(", ").append(triggerKeysExpression).append(" as key");
        // unidirectional will cache last right join(not trigger)
        sb.append(" from ").append(triggerName).append(".std:lastevent()").append(" as ").append(triggerName);
//        sb.append(" from ").append(triggerName).append(" as ").append(triggerName);

        for (Identifier id : meta.getSrcVariableMetasID()) {
            if (id.equals(meta.getTrigger())) {
                continue;
            }
            String srcName = id.getKeys().get(1);
            if (!wrapper.isInBatchQueryIds(id)) {
                sb.append(" left outer join ");
                sb.append(srcName).append(".std:lastevent() as ").append(srcName).append(" on ")
                        .append(triggerKeysExpression).append("=").append(srcName).append(".key");
            }
        }

        if (meta.getPropertyCondition() != null) {
            sb.append(genWhereClause(wrapper.getBatchQueryIds(), triggerKeysExp, meta.getPropertyCondition()));
        }

        EsperEPL epl = new EsperEPL(genEPLName(meta), sb.toString(), true, false, getPriorityInEPL(meta));
        return Arrays.asList(epl);
    }

    private String wrapTriggerKeys(String triggerKeysEPL) {
        return String.format("OperationExt.concatAttributes(%s)", triggerKeysEPL);
    }

    private String genTriggerKeysString(Identifier trigger, List<Property> triggerProperties) {
        if (triggerProperties == null || triggerProperties.isEmpty()) {
            throw new RuntimeException("at least one trigger property");
        }
        if (triggerProperties.size() == 1) {
            return String.format("%s.%s", trigger.getKeys().get(1), triggerProperties.get(0).getName());
        }

        StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (Property p : triggerProperties) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(trigger.getKeys().get(1)).append(".").append(p.getName());
        }
        return sb.toString();
    }
}
