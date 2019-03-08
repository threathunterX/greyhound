package com.threathunter.greyhound.server.esper.eplgen.property;

import com.threathunter.common.Identifier;
import com.threathunter.greyhound.server.esper.eplgen.EPLExpressionGenerator;
import com.threathunter.greyhound.server.esper.eplgen.EPLGenUtil;
import com.threathunter.greyhound.server.esper.extension.SlotVariableQueryHelper;
import com.threathunter.model.Property;
import com.threathunter.model.PropertyCondition;
import com.threathunter.util.ClassBasedRegistry;
import com.threathunter.variable.condition.*;
import com.threathunter.variable.exception.NotSupportException;

import java.util.ArrayList;
import java.util.List;

/**
 * Generator that can generate esper expression from the PropertyCondition.
 *
 * @author Wen Lu
 */
public class PropertyConditionEPLGenerator<C extends PropertyCondition> implements EPLExpressionGenerator {
    private static final ClassBasedRegistry<PropertyCondition, PropertyConditionEPLGenerator> registry =
            new ClassBasedRegistry<>(PropertyCondition.class);

    static {
        registerCondition(StringPropertyCondition.class, StringConditionEPLGenerator.class);
        registerCondition(LongPropertyCondition.class, LongConditionEPLGenerator.class);
        registerCondition(CompoundCondition.class, CompoundConditionEPLGenerator.class);
        registerCondition(DoublePropertyCondition.class, DoubleConditionEPLGenerator.class);
        registerCondition(GeneralPropertyCondition.class, GeneralConditionEPLGenerator.class);
        registerCondition(IPPropertyCondition.class, IPConditionEPLGenerator.class);
    }

    private static boolean isContainsChinese(String param) {
        for (int i = 0; i < param.length(); ) {
            int codePoint = param.codePointAt(i);
            i += Character.charCount(codePoint);
            if (Character.UnicodeScript.of(codePoint) == Character.UnicodeScript.HAN) {
                return true;
            }
        }
        return false;
    }

    public static String temporateHelper(String[] array) {
        List<String> result = new ArrayList<>();
        for (String s : array) {
            if (isContainsChinese(s)) {
                result.add("'" + s + "'");
            } else {
                result.add(s);
            }
        }
        return String.join(",", result);

    }

    /**
     * Register the special generator for special conditions.
     *
     */
    public static void registerCondition(Class<? extends PropertyCondition> c, Class<? extends PropertyConditionEPLGenerator> g) {
        registry.register(c, g);
    }

    public static String genExpressionFromCondition(List<Identifier> batchSlotIds, String joinKeysExpression, PropertyCondition c) {
        Class<? extends PropertyConditionEPLGenerator> g = registry.get(c.getClass());

        try {
            PropertyConditionEPLGenerator gen = g.newInstance();
            gen.setCondition(c);
            gen.setJoinKeysExpression(joinKeysExpression);
            gen.setBatchSlotIds(batchSlotIds);
            return gen.generateEPLExpression();
        } catch (Exception ex) {
            throw new RuntimeException("error in epl generation.", ex);
        }
    }

    private C condition;

    private List<Identifier> batchSlotIds;

    private String joinKeysExpression;

    public PropertyConditionEPLGenerator(C condition) {
        this.condition = condition;
    }

    public PropertyConditionEPLGenerator() {
    }

    public C getCondition() {
        return condition;
    }

    public void setCondition(C condition) {
        this.condition = condition;
    }

    public void setBatchSlotIds(List<Identifier> batchSlotIds) {
        this.batchSlotIds = batchSlotIds;
    }

    protected String getJoinKeysExpression() {
        return joinKeysExpression;
    }

    public void setJoinKeysExpression(String joinKeysExpression) {
        this.joinKeysExpression = joinKeysExpression;
    }

    protected List<Identifier> getBatchSlotIds() {
        return this.batchSlotIds;
    }

    @Override
    public String generateEPLExpression() {
        return "(true)";
    }

    public static class CompoundConditionEPLGenerator extends PropertyConditionEPLGenerator<CompoundCondition> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            CompoundCondition c = getCondition();
            List<PropertyCondition> subConditions = c.getConditions();

            if (c instanceof CompoundCondition.NotPropertyCondition) {
                result.append("(not ");
                result.append(genExpressionFromCondition(getBatchSlotIds(), getJoinKeysExpression(), subConditions.get(0)));
                result.append(")");
                return result.toString();
            }

            // for and/or condition.
            result.append("(");
            boolean first = true;
            for(PropertyCondition subCondtition : c.getConditions()) {
                if (!first) {
                    if (c instanceof CompoundCondition.AndPropertyCondition) {
                        result.append(" and ");
                    } else if (c instanceof CompoundCondition.OrPropertyCondition) {
                        result.append((" or "));
                    } else {
                        throw new NotSupportException();
                    }
                } else {
                    first = false;
                }
                result.append(genExpressionFromCondition(getBatchSlotIds(), getJoinKeysExpression(), subCondtition));
            }
            result.append(")");
            return result.toString();
        }
    }

    public static class DoubleConditionEPLGenerator extends PropertyConditionEPLGenerator<DoublePropertyCondition> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            DoublePropertyCondition c = getCondition();
            Property p = c.getSrcProperties().get(0);
            Double param = (Double) c.getParam();

            String leftEPL;
            if (getBatchSlotIds() != null && getBatchSlotIds().contains(p.getIdentifier())) {
                leftEPL = SlotVariableQueryHelper.genEPLExpression(p.getIdentifier(), getJoinKeysExpression());
            } else {
                leftEPL = EPLGenUtil.genQualifiedName(p);
            }
            result.append("(");
            if (c instanceof DoublePropertyCondition.DoubleSmallerThanPropertyCondition) {
                result.append(leftEPL).append("<").append(param);
            } else if (c instanceof DoublePropertyCondition.DoubleSmallerEqualsPropertyCondition) {
                result.append(leftEPL).append("<=").append(param);
            } else if (c instanceof DoublePropertyCondition.DoubleBiggerThanPropertyCondition) {
                result.append(leftEPL).append(">").append(param);
            } else if (c instanceof DoublePropertyCondition.DoubleBiggerEqualsPropertyCondition) {
                result.append(leftEPL).append(">=").append(param);
            } else if (c instanceof DoublePropertyCondition.DoubleEqualsPropertyCondition) {
                result.append(leftEPL).append("=").append(param);
            } else if (c instanceof DoublePropertyCondition.DoubleNotEqualsPropertyCondition) {
                result.append(leftEPL).append("!=").append(param);
            } else {
                throw new NotSupportException();
            }
            result.append(")");
            return result.toString();
        }
    }

    public static class LongConditionEPLGenerator extends PropertyConditionEPLGenerator<LongPropertyCondition> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            LongPropertyCondition c = getCondition();
            Property p = c.getSrcProperties().get(0);
            Long param = (Long) c.getParam();

            String leftEPL;
            if (getBatchSlotIds() != null && getBatchSlotIds().contains(p.getIdentifier())) {
                leftEPL = SlotVariableQueryHelper.genEPLExpression(p.getIdentifier(), getJoinKeysExpression());
            } else {
                leftEPL = EPLGenUtil.genQualifiedName(p);
            }
            result.append("(");
            if (c instanceof LongPropertyCondition.LongSmallerThanPropertyCondition) {
                result.append(leftEPL).append("<").append(param);
            } else if (c instanceof LongPropertyCondition.LongSmallerEqualsPropertyCondition) {
                result.append(leftEPL).append("<=").append(param);
            } else if (c instanceof LongPropertyCondition.LongBiggerThanPropertyCondition) {
                result.append(leftEPL).append(">").append(param);
            } else if (c instanceof LongPropertyCondition.LongBiggerEqualsPropertyCondition) {
                result.append(leftEPL).append(">=").append(param);
            } else if (c instanceof LongPropertyCondition.LongEqualsPropertyCondition) {
                result.append(leftEPL).append("=").append(param);
            } else if (c instanceof LongPropertyCondition.LongNotEqualsPropertyCondition) {
                result.append(leftEPL).append("!=").append(param);
            } else {
                throw new NotSupportException();
            }
            result.append(")");
            return result.toString();
        }
    }

    public static class StringConditionEPLGenerator extends PropertyConditionEPLGenerator<StringPropertyCondition> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            StringPropertyCondition c = getCondition();
            Property p = c.getSrcProperties().get(0);
            String param = "'"+c.getParam()+"'";
            result.append("(");
            if (c instanceof StringPropertyCondition.StringContainsByPropertyCondition) {
                result.append("OperationExt.isStringIn(").append(param).append(", ").append(EPLGenUtil.genQualifiedName(p)).append(")");
            } else if (c instanceof StringPropertyCondition.StringNotContainsByPropertyCondition) {
                result.append("not OperationExt.isStringIn(").append(param).append(", ").append(EPLGenUtil.genQualifiedName(p)).append(")");
            } else if (c instanceof StringPropertyCondition.StringContainsPropertyCondition) {
                result.append(EPLGenUtil.genQualifiedName(p)).append(".contains(").append(param).append(")");
            } else if (c instanceof StringPropertyCondition.StringNotContainsPropertyCondition) {
                result.append("not ").append(EPLGenUtil.genQualifiedName(p)).append(".contains(").append(param).append(")");
            } else if (c instanceof StringPropertyCondition.StringEqualsPropertyCondition) {
                result.append(EPLGenUtil.genQualifiedName(p)).append("=").append(param);
            } else if (c instanceof StringPropertyCondition.StringNotEqualsPropertyCondition) {
                result.append(EPLGenUtil.genQualifiedName(p)).append("!=").append(param);
            } else if (c instanceof StringPropertyCondition.StringMatchPropertyCondition) {
                param = "'" + ((String) c.getParam()).replaceAll("\\\\", "\\\\\\\\").replaceAll("\\'", "\\\\'") + "'";
                result.append(EPLGenUtil.genQualifiedName(p)).append(" regexp ").append(param);
            } else if (c instanceof StringPropertyCondition.StringNotMatchPropertyCondition) {
                param = "'" + ((String) c.getParam()).replaceAll("\\\\", "\\\\\\\\").replaceAll("\\'", "\\\\'") + "'";
                result.append(EPLGenUtil.genQualifiedName(p)).append(" not regexp ").append(param);
            } else if (c instanceof StringPropertyCondition.StringStartwithPropertyCondition) {
                result.append(EPLGenUtil.genQualifiedName(p)).append(" like '").append(c.getParam()).append("%'");
            } else if (c instanceof StringPropertyCondition.StringNotStartwithPropertyCondition) {
                result.append(EPLGenUtil.genQualifiedName(p)).append(" not like '").append(c.getParam()).append("%'");
            } else if (c instanceof StringPropertyCondition.StringEndwithPropertyCondition) {
                result.append(EPLGenUtil.genQualifiedName(p)).append(" like '%").append(c.getParam()).append("'");
            } else if (c instanceof StringPropertyCondition.StringNotEndwithPropertyCondition) {
                result.append(EPLGenUtil.genQualifiedName(p)).append(" not like '%").append(c.getParam()).append("'");
            } else {
                throw new NotSupportException();
            }
            result.append(")");
            return result.toString();
        }
    }

    public static class IPConditionEPLGenerator extends PropertyConditionEPLGenerator<IPPropertyCondition> {
        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            IPPropertyCondition c = getCondition();
            Property p = c.getSrcProperties().get(0);
            String[] params = c.getRightParams();
            String paramType = c.getParamType();
            result.append("(");
            if (c instanceof IPPropertyCondition.IPEqualsPropertyCondition) {
                result.append("com.threathunter.greyhound.server.esper.extension.CommonFilterHelper.ipEqual(").append(EPLGenUtil.genQualifiedName(p))
                        .append(", '").append(paramType).append("', ").append(temporateHelper(params)).append(")");
            } else if (c instanceof IPPropertyCondition.IPNotEqualsPropertyCondition) {
                result.append("not com.threathunter.greyhound.server.esper.extension.CommonFilterHelper.ipEqual(").append(EPLGenUtil.genQualifiedName(p))
                        .append(", '").append(paramType).append("', ").append(temporateHelper(params)).append(")");
            } else if (c instanceof IPPropertyCondition.IPContainsPropertyCondition) {
                result.append("com.threathunter.greyhound.server.esper.extension.CommonFilterHelper.ipContains(").append(EPLGenUtil.genQualifiedName(p))
                        .append(", '").append(paramType).append("', ").append(temporateHelper(params)).append(")");
            } else if (c instanceof IPPropertyCondition.IPNotContainsPropertyCondition) {
                result.append("not com.threathunter.greyhound.server.esper.extension.CommonFilterHelper.ipContains(").append(EPLGenUtil.genQualifiedName(p))
                        .append(", '").append(paramType).append("', ").append(temporateHelper(params)).append(")");
            } else {
                throw new NotSupportException();
            }
            result.append(")");
            return result.toString();
        }
    }

    public static class GeneralConditionEPLGenerator extends PropertyConditionEPLGenerator<GeneralPropertyCondition> {

        @Override
        public String generateEPLExpression() {
            GeneralPropertyCondition c = getCondition();

            String result = c.getConfig();
            List<Property> properties = c.getSrcProperties();
            if (properties != null) {
                for (int i = 0; i < properties.size(); i++) {
                    Property p = properties.get(i);
                    if (p == null) continue;
                    result = result.replaceAll("#\\{param" + i +"\\}", EPLGenUtil.genQualifiedName(p));
                }
            }
            return result;
        }
    }
}
