package com.threathunter.greyhound.server.esper.eplgen.property;

import com.threathunter.greyhound.server.esper.eplgen.EPLExpressionGenerator;
import com.threathunter.greyhound.server.esper.eplgen.EPLGenUtil;
import com.threathunter.model.Property;
import com.threathunter.model.PropertyReduction;
import com.threathunter.util.ClassBasedRegistry;
import com.threathunter.variable.exception.NotSupportException;
import com.threathunter.variable.reduction.*;

import java.util.List;

/**
 * Generator that can generate esper expression from the PropertyReduction.
 *
 * @author Wen Lu
 */
public class PropertyReductionEPLGenerator<R extends PropertyReduction> implements EPLExpressionGenerator {
    private static final ClassBasedRegistry<PropertyReduction, PropertyReductionEPLGenerator> registry =
            new ClassBasedRegistry<>(PropertyReduction.class);

    static {
        registerReduction(LongPropertyReduction.class, LongReductionEPLGenerator.class);
        registerReduction(DoublePropertyReduction.class, DoubleReductionEPLGenerator.class);
        registerReduction(StringPropertyReduction.class, StringReductionEPLGenerator.class);
        registerReduction(WildcardPropertyReduction.class, WildcardReductionEPLGenerator.class);
        registerReduction(MultiplePropertyReduction.class, MultipleReductionEPLGenerator.class);
    }

    /**
     * Register the special generator for special reduction.
     *
     */
    public static void registerReduction(Class<? extends PropertyReduction> r, Class<? extends PropertyReductionEPLGenerator> g) {
        registry.register(r, g);
    }

    public static String genExpressionFromReduction(PropertyReduction r) {
        Class<? extends PropertyReductionEPLGenerator> g = registry.get(r.getClass());
        if (g == null) {
            return null;
        }

        try {
            PropertyReductionEPLGenerator gen = g.newInstance();
            gen.setReduction(r);
            return gen.generateEPLExpression();
        } catch (Exception ex) {
            throw new RuntimeException("error in epl generation.", ex);
        }
    }

    private R reduction;

    public PropertyReductionEPLGenerator(R reduction) {
        this.reduction = reduction;
    }

    public PropertyReductionEPLGenerator() {
    }

    public R getReduction() {
        return reduction;
    }

    public void setReduction(R reduction) {
        this.reduction = reduction;
    }

    @Override
    public String generateEPLExpression() {
        return "";
    }

    public static class DoubleReductionEPLGenerator extends PropertyReductionEPLGenerator<DoublePropertyReduction> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            DoublePropertyReduction r = getReduction();
            Property src = r.getSrcProperties().get(0);
            Property dest = r.getDestProperty();
            if (r instanceof DoublePropertyReduction.DoubleMaxPropertyReduction) {
                result.append("max(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleMinPropertyReduction) {
                result.append("min(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleSumPropertyReduction) {
                result.append("sum(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleCountPropertyReduction) {
                result.append("count(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleDistinctCountPropertyReduction) {
                result.append("count(distinct ").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleAvgPropertyReduction) {
                result.append("avg(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleFirstPropertyReduction) {
                result.append("first(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleLastPropertyReduction) {
                result.append("last(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleStddevPropertyReduction) {
                result.append("stddev(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleRangePropertyReduction) {
                result.append("last(").append(EPLGenUtil.genQualifiedName(src)).append(")");
                result.append("-");
                result.append("first(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof DoublePropertyReduction.DoubleAmplitudePropertyReduction) {
                result.append("max(").append(EPLGenUtil.genQualifiedName(src)).append(")");
                result.append("-");
                result.append("min(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else {
                throw new NotSupportException();
            }

            result.append(" as ").append(dest.getName());
            return result.toString();
        }
    }

    public static class LongReductionEPLGenerator extends PropertyReductionEPLGenerator<LongPropertyReduction> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            LongPropertyReduction r = getReduction();
            Property src = r.getSrcProperties().get(0);
            Property dest = r.getDestProperty();
            if (r instanceof LongPropertyReduction.LongMaxPropertyReduction) {
                result.append("max(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongMinPropertyReduction) {
                result.append("min(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongSumPropertyReduction) {
                result.append("sum(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongCountPropertyReduction) {
                result.append("count(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongDistinctCountPropertyReduction) {
                result.append("count(distinct ").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongAvgPropertyReduction) {
                result.append("avg(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongFirstPropertyReduction) {
                result.append("first(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongLastPropertyReduction) {
                result.append("last(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongStddevPropertyReduction) {
                result.append("stddev(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongRangePropertyReduction) {
                result.append("last(").append(EPLGenUtil.genQualifiedName(src)).append(")");
                result.append("-");
                result.append("first(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof LongPropertyReduction.LongAmplitudePropertyReduction) {
                result.append("max(").append(EPLGenUtil.genQualifiedName(src)).append(")");
                result.append("-");
                result.append("min(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else {
                throw new NotSupportException();
            }

            result.append(" as ").append(dest.getName());
            return result.toString();
        }
    }

    public static class StringReductionEPLGenerator extends PropertyReductionEPLGenerator<StringPropertyReduction> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            StringPropertyReduction r = getReduction();
            Property src = r.getSrcProperties().get(0);
            Property dest = r.getDestProperty();
            if (r instanceof StringPropertyReduction.StringCountPropertyReduction) {
                result.append("count(").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else if (r instanceof StringPropertyReduction.StringDistinctCountPropertyReduction) {
                result.append("count(distinct ").append(EPLGenUtil.genQualifiedName(src)).append(")");
            } else {
                throw new NotSupportException();
            }

            result.append(" as ").append(dest.getName());
            return result.toString();
        }
    }

    public static class WildcardReductionEPLGenerator extends PropertyReductionEPLGenerator<WildcardPropertyReduction> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            WildcardPropertyReduction r = getReduction();
            Property dest = r.getDestProperty();
            if (r instanceof WildcardPropertyReduction.WildcardCountPropertyReduction) {
                result.append("count(*)");
            } else if (r instanceof WildcardPropertyReduction.WildcardDistinctCountPropertyReduction) {
                result.append("count(distinct *)");
            } else {
                throw new NotSupportException();
            }

            result.append(" as ").append(dest.getName());
            return result.toString();
        }
    }

    public static class MultipleReductionEPLGenerator extends PropertyReductionEPLGenerator<MultiplePropertyReduction> {

        @Override
        public String generateEPLExpression() {
            StringBuilder result = new StringBuilder();
            MultiplePropertyReduction r = getReduction();

            List<Property> srcProperties = r.getSrcProperties();
            if (r instanceof MultiplePropertyReduction.MultipleCountPropertyReduction) {
                result.append("sum(");
                if (srcProperties == null || srcProperties.isEmpty()) {
                    result.append("value");
                } else {
                    boolean first = true;
                    for (Property p : srcProperties) {
                        if (first) {
                            first = false;
                        } else {
                            result.append(", ");
                        }
                        result.append(EPLGenUtil.genQualifiedValue(p));
                    }
                }
            } else if (r instanceof MultiplePropertyReduction.MultipleDistinctCountPropertyReduction) {
                result.append("count(distinct ");

                if (srcProperties == null || srcProperties.isEmpty()) {
                    result.append("value");
                } else {
                    boolean first = true;
                    for (Property p : srcProperties) {
                        if (first) {
                            first = false;
                        } else {
                            result.append(", ");
                        }
                        result.append(p.getName());
                    }
                }
            } else {
                throw new NotSupportException();
            }

            result.append(")");

            Property dest = r.getDestProperty();
            result.append(" as ").append(dest.getName());
            return result.toString();
        }
    }
}
