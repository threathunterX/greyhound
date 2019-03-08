package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.Identifier;
import com.threathunter.common.NamedType;
import com.threathunter.greyhound.server.VariableMetaWrapper;
import com.threathunter.greyhound.server.esper.EsperEPL;
import com.threathunter.greyhound.server.esper.eplgen.property.PropertyConditionEPLGenerator;
import com.threathunter.greyhound.server.esper.eplgen.property.PropertyMappingEPLGenerator;
import com.threathunter.greyhound.server.esper.eplgen.property.PropertyReductionEPLGenerator;
import com.threathunter.model.*;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by daisy on 17/8/25.
 */
public abstract class VariableEPLGenerator<V extends VariableMeta> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VariableEPLGenerator.class);

    public abstract List<EsperEPL> generateEPL(VariableMetaWrapper<V> wrapper);

    public static List<EsperEPL> getEPLs(List<VariableMetaWrapper> metasWrappers){
        List<EsperEPL> epls = new ArrayList<>();
        metasWrappers.forEach(wrapper -> {
            Class<? extends VariableEPLGenerator> g = VariableEPLGeneratorRegistry.getGenerator(wrapper.getMeta().getClass());
            try {
                VariableEPLGenerator generator = g.newInstance();
                epls.addAll(generator.generateEPL(wrapper));
            } catch (Exception e) {
                LOGGER.error("[greyhoung:esper] generate epl error", e);
            }

        });
        epls.forEach(epl -> epl.setNeedListen(true));
        return epls;
    }

    public static List<EsperEPL> getEPLs(VariableMetaWrapper wrapper) {
        Class<? extends VariableEPLGenerator> g = VariableEPLGeneratorRegistry.getGenerator(wrapper.getMeta().getClass());
        try {
            VariableEPLGenerator generator = g.newInstance();
            List<EsperEPL> esperEPLs = generator.generateEPL(wrapper);

            esperEPLs.forEach(epl -> epl.setNeedListen(true));
            return esperEPLs;
        } catch (Exception e) {
            LOGGER.error("[greyhoung:esper] generate epl error", e);
            return new ArrayList<>();
        }
    }

    protected String genEPLName(VariableMeta meta) {
        return String.format("event__%s__%s", meta.getApp(), meta.getName());
    }

    protected String genGeneralProperty(String app, String name) {
        return String.format("'%s' as app, '%s' as name, current_timestamp as timestamp", app, name);
    }

    protected String genAnnotation(VariableMeta meta) {
        StringBuilder sb = new StringBuilder();
        long dataWindow = meta.getTtl();

        if (dataWindow > 0) {
            int reclaimGroupAged = (int)(dataWindow);
            int reclaimGroupFreq = (int)(dataWindow/2);
            if (reclaimGroupFreq < 120) {
                reclaimGroupFreq = 120; // at least 2 minutes so the sweep is not too frequent
            }
            sb.append("@Hint('reclaim_group_aged=").append(reclaimGroupAged)
                    .append(",reclaim_group_freq=").append(reclaimGroupFreq)
                    .append("') ");
        }

        sb.append("@Priority(").append(getPriorityInEPL(meta)).append(") ");
        return sb.toString();
    }

    /**
     * Generate the insert into part
     *
     */
    protected String genInsertInto(VariableMeta meta) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into `").append(meta.getName()).append("` ");
        return sb.toString();
    }

    protected String genPropertyMappings(List<PropertyMapping> mappings) {
        if (mappings == null || mappings.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        MutableBoolean first = new MutableBoolean(true);
        mappings.forEach(mapping -> {
            if (!first.getValue()) {
                builder.append(", ");
            } else {
                first.setFalse();
            }
            builder.append(PropertyMappingEPLGenerator.genExpressionFromMapping(mapping));
        });
        return builder.toString();
    }

    protected String genKey(List<Property> groupKeys) {
        return this.genKey(groupKeys, null);
    }

    protected String genKey(List<Property> groupKeys, List<Identifier> srcIdentifiers) {
        if (groupKeys == null || groupKeys.isEmpty()) {
            return "'' as key";
        }
        StringBuilder sb = new StringBuilder();
        if (groupKeys.size() == 1) {
            String key = groupKeys.get(0).getName();
            if (srcIdentifiers == null || srcIdentifiers.size() <= 1) {
                sb.append(groupKeys.get(0).getIdentifier().getKeys().get(1)).append(".")
                        .append(key);
            } else {
                sb.append("coalesce(").append(srcIdentifiers.get(0).getKeys().get(1)).append(".").append(key).append(", ")
                        .append(srcIdentifiers.get(1).getKeys().get(1)).append(".").append(key).append(")");
            }
            sb.append(" as key");
            return sb.toString();
        }
        boolean first = true;
        sb.append("OperationExt.concatAttributes(");
        for (Property p : groupKeys) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(p.getIdentifier().getKeys().get(1)).append(".").append(p.getName());
        }
        sb.append(")");
        sb.append(" as key");
        return sb.toString();
    }

    protected String genGroupKeysProperty(List<Property> groupKeys) {
        if (groupKeys == null || groupKeys.size() <= 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Property p : groupKeys) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(p.getIdentifier().getKeys().get(1)).append(".").append(p.getName());
            sb.append(" as ");
            sb.append(p.getName());
        }

        return sb.toString();
    }

    protected String genGroupByExpression(List<Property> groupbyKeys) {
        if (groupbyKeys == null || groupbyKeys.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("group by");
        groupbyKeys.forEach(property -> {
            builder.append(" ").append(property.getIdentifier().getKeys().get(1)).append(".").append(property.getName());
        });

        return builder.toString();
    }

    protected String genWindowedMappingExpression(List<PropertyMapping> mappings) {
        if (mappings == null || mappings.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        MutableBoolean first = new MutableBoolean(true);
        mappings.forEach(mapping -> {
            if (!first.getValue()) {
                builder.append(", ");
            } else {
                first.setFalse();
            }
            builder.append(mapping.getSrcProperties().get(0).getName()).append(" as ").append(
                        mapping.getDestProperty().getName());
        });

        return builder.toString();
    }

    private String genMappingExpression(List<PropertyMapping> mappings) {
        if (mappings == null || mappings.isEmpty()) {
            return "";
        }
        MutableBoolean first = new MutableBoolean(true);
        StringBuilder builder = new StringBuilder();
        mappings.forEach(mapping -> {
            if (!first.getValue()) {
                builder.append(", ");
            } else {
                first.setFalse();
            }
            Property srcProperty = mapping.getSrcProperties().get(0);
            builder.append(srcProperty.getIdentifier().getUnfoldedName()).append(".").append(srcProperty.getName())
                    .append(" as ").append(mapping.getDestProperty().getName());
        });
        return builder.toString();
    }

    protected String genExpressionsClause(VariableMeta meta, boolean needKey, boolean needValue) {
        List<String> expressions = new ArrayList<>(); // generated expressions
        Set<String> destPropertyNames = new HashSet<>(); // generated property names in expressions
        if (meta.getPropertyMappings() != null) {
            meta.getPropertyMappings().forEach(mapping -> {
                String exp = PropertyMappingEPLGenerator.genExpressionFromMapping(mapping);
                expressions.add(exp);
                destPropertyNames.add(mapping.getDestProperty().getName());
            });
        }
        if (meta.getPropertyReduction() != null) {
            PropertyReduction reduction = meta.getPropertyReduction();
            String exp = PropertyReductionEPLGenerator.genExpressionFromReduction(reduction);
            if (reduction.getDestProperty().getName().equals("value") && reduction.getDestProperty().getType().equals(NamedType.DOUBLE))
                exp = valueShouldBeDouble(exp);

            expressions.add(exp);
            destPropertyNames.add(reduction.getDestProperty().getName());
        }

        if (expressions.size() != destPropertyNames.size()) {
            // the dest property could conflict
            throw new RuntimeException("conflict property:" + expressions);
        }
        if (needValue && !destPropertyNames.contains("value")) {
            expressions.add("1.0 as value"); // use 1 by default variable value
            destPropertyNames.add("value");
        }
        if (needKey && !destPropertyNames.contains("key")) {
            if (meta.getGroupKeys() != null) {
                expressions.add(generateGroupKeysExpression(meta.getGroupKeys()));
            } else {
                expressions.add("'' as key");
            }
            destPropertyNames.add("key");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("'").append(meta.getApp()).append("' as app");
        sb.append(", '").append(meta.getName()).append("' as name");
        sb.append(", current_timestamp as timestamp ");
        for(String exp : expressions) {
            sb.append(", ").append(exp);
        }

        return sb.toString();
    }


    /**
     * Generate the where clause in epl statement.
     *
     */
    protected String genWhereClause(List<Identifier> batchSlotIds, String joinKeysExpression, PropertyCondition c) {
        if (c == null) {
            return "";
        } else {
            return " where " + PropertyConditionEPLGenerator.genExpressionFromCondition(batchSlotIds, joinKeysExpression, c) + " ";
        }
    }

    protected String genHavingClause(List<Identifier> batchSlotIds, String joinKeysExpression, PropertyCondition c) {
        if (c == null) {
            return "";
        } else {
            return " having " + PropertyConditionEPLGenerator.genExpressionFromCondition(batchSlotIds, joinKeysExpression, c) + " ";
        }
    }

    private String valueShouldBeDouble(String expression) {
        if (!expression.endsWith(" as value"))
            return expression;

        int idx = expression.lastIndexOf(" as value");
        if (idx > 0) {
            return "1.0*(" + expression.substring(0, idx) + ") as value";
        } else {
            return expression;
        }
    }

    /**
     * Return the priority in esper.
     *
     * In esper, the bigger value means the higher priority, which is
     * the opposite to our variable behavior,
     *
     */
    protected int getPriorityInEPL(VariableMeta meta) {
        return (1000 - meta.getPriority());
    }

    private String generateGroupKeysExpression(List<Property> groupKeys) {
        StringBuilder sb = new StringBuilder();
        if (groupKeys == null || groupKeys.isEmpty()) {
            sb.append("''");
        } else if (groupKeys.size() > 7) {
            sb.append("''");
        } else if (groupKeys.size() == 1) {
            sb.append(genNamedKey(groupKeys.get(0).getName(), groupKeys.get(0).getIdentifier()));
        } else {
            sb.append("OperationExt.concatAttributes(");
            sb.append(genNamedKey(groupKeys.get(0).getName(), groupKeys.get(0).getIdentifier()));
            for (int i = 0; i < groupKeys.size(); i++) {
                sb.append(",").append(genNamedKey(groupKeys.get(i).getName(), groupKeys.get(i).getIdentifier()));
            }
            sb.append(")");
        }
        sb.append(" as key");
        return sb.toString();
    }

    private String genNamedKey(String keyName, Identifier srcId) {
        if (srcId == null) {
            return keyName;
        }
        return String.format("%s.%s", srcId.getKeys().get(1), keyName);
    }
}
