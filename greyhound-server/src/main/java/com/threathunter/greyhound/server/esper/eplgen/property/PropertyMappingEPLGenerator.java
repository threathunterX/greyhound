package com.threathunter.greyhound.server.esper.eplgen.property;

import com.threathunter.common.Identifier;
import com.threathunter.greyhound.server.esper.eplgen.EPLExpressionGenerator;
import com.threathunter.greyhound.server.esper.eplgen.EPLGenUtil;
import com.threathunter.model.Property;
import com.threathunter.model.PropertyMapping;
import com.threathunter.util.ClassBasedRegistry;
import com.threathunter.variable.mapping.*;

import java.util.List;

/**
 * Generator that can generate esper expression from the PropertyMapping.
 *
 * @author Wen Lu
 */
public class PropertyMappingEPLGenerator<M extends PropertyMapping> implements EPLExpressionGenerator {
    private static final ClassBasedRegistry<PropertyMapping, PropertyMappingEPLGenerator> registry =
            new ClassBasedRegistry<>(PropertyMapping.class);

    static {
        registerMapping(BoolValuePropertyMapping.class, BoolValueMappingEPLGenerator.class);
        registerMapping(LongValuePropertyMapping.class, LongValueMappingEPLGenerator.class);
        registerMapping(DoubleValuePropertyMapping.class, DoubleValueMappingEPLGenerator.class);
        registerMapping(DirectPropertyMapping.class, DirectMappingEPLGenerator.class);
        registerMapping(StringValuePropertyMapping.class, StringValueMappingEPLGenerator.class);
        registerMapping(ConcatPropertyMapping.class, ConcatMappingEPLGenerator.class);
        registerMapping(VariablePropertyMapping.class, VariableValueMappingEPLGenerator.class);
    }

    /**
     * Register the special generator for special mapping.
     *
     */
    public static void registerMapping(Class<? extends PropertyMapping> m, Class<? extends PropertyMappingEPLGenerator> g) {
        registry.register(m, g);
    }

    public static String genExpressionFromMapping(PropertyMapping m) {
        Class<? extends PropertyMappingEPLGenerator> g = registry.get(m.getClass());
        if (g == null) {
            return null;
        }

        try {
            PropertyMappingEPLGenerator gen = g.newInstance();
            gen.setMapping(m);
            return gen.generateEPLExpression();
        } catch (Exception ex) {
            throw new RuntimeException("error in epl generation.", ex);
        }
    }

    private M mapping;

    public PropertyMappingEPLGenerator(M mapping) {
        this.mapping = mapping;
    }

    public PropertyMappingEPLGenerator() {
    }

    public M getMapping() {
        return mapping;
    }

    public void setMapping(M mapping) {
        this.mapping = mapping;
    }

    @Override
    public String generateEPLExpression() {
        return "";
    }

    public static class DirectMappingEPLGenerator extends PropertyMappingEPLGenerator<DirectPropertyMapping> {

        @Override
        public String generateEPLExpression() {
            DirectPropertyMapping m = getMapping();
            Property src = m.getSrcProperties().get(0);
            Property dest = m.getDestProperty();
            StringBuilder sb = new StringBuilder();
            sb.append(EPLGenUtil.genQualifiedName(src)).append(" as ").append(dest.getName());
            return sb.toString();
        }
    }

    public static class StringValueMappingEPLGenerator extends PropertyMappingEPLGenerator<StringValuePropertyMapping> {

        @Override
        public String generateEPLExpression() {
            StringValuePropertyMapping m = getMapping();
            String param = m.getParam();
            Property dest = m.getDestProperty();
            StringBuilder sb = new StringBuilder();
            sb.append("'").append(param).append("'").append(" as ").append(dest.getName());
            return sb.toString();
        }
    }

    public static class DoubleValueMappingEPLGenerator extends PropertyMappingEPLGenerator<DoubleValuePropertyMapping> {

        @Override
        public String generateEPLExpression() {
            DoubleValuePropertyMapping m = getMapping();
            double param = m.getParam();
            Property dest = m.getDestProperty();
            StringBuilder sb = new StringBuilder();
            sb.append(param).append(" as ").append(dest.getName());
            return sb.toString();
        }
    }

    public static class LongValueMappingEPLGenerator extends PropertyMappingEPLGenerator<LongValuePropertyMapping> {

        @Override
        public String generateEPLExpression() {
            LongValuePropertyMapping m = getMapping();
            long param = m.getParam();
            Property dest = m.getDestProperty();
            StringBuilder sb = new StringBuilder();
            sb.append(param).append(" as ").append(dest.getName());
            return sb.toString();
        }
    }

    public static class BoolValueMappingEPLGenerator extends PropertyMappingEPLGenerator<BoolValuePropertyMapping> {

        @Override
        public String generateEPLExpression() {
            BoolValuePropertyMapping m = getMapping();
            boolean param = m.getParam();
            String paramStr = "true";
            if (!param) paramStr = "false";
            Property dest = m.getDestProperty();
            StringBuilder sb = new StringBuilder();
            sb.append(paramStr).append(" as ").append(dest.getName());
            return sb.toString();
        }
    }

    public static class VariableValueMappingEPLGenerator extends PropertyMappingEPLGenerator<VariablePropertyMapping> {

        @Override
        public String generateEPLExpression() {
            VariablePropertyMapping m = getMapping();
            Identifier srcVariable = m.getSrcProperties().get(0).getIdentifier();
            Property dest = m.getDestProperty();
            StringBuilder sb = new StringBuilder();
            sb.append(srcVariable.getKeys().get(1)).append(".* as ").append(dest.getName());
            return sb.toString();
        }
    }

    public static class ConcatMappingEPLGenerator extends PropertyMappingEPLGenerator<ConcatPropertyMapping> {

        @Override
        public String generateEPLExpression() {
            StringBuilder sb = new StringBuilder();
            ConcatPropertyMapping m = getMapping();
            List<Property> srcProperties = m.getSrcProperties();
            Property dest = m.getDestProperty();
            if (srcProperties == null || srcProperties.isEmpty()) {
                // no key
                sb.append("''");
            } else if (srcProperties.size() == 1) {
                // one single value
                sb.append(EPLGenUtil.genQualifiedName(srcProperties.get(0)));
            } else if (srcProperties.size() > 7 ){
                // we can't support that more
                sb.append("''");
            } else {
                // 2-7 keys
                sb.append("OperationExt.concatAttributes(");
                sb.append(EPLGenUtil.genQualifiedName(srcProperties.get(0)));
                for(int i = 1; i < srcProperties.size(); i++) {
                    sb.append(", ").append(EPLGenUtil.genQualifiedName(srcProperties.get(i)));
                }
                sb.append(")");
            }
            sb.append(" as ").append(dest.getName());
            return sb.toString();
        }
    }
}
