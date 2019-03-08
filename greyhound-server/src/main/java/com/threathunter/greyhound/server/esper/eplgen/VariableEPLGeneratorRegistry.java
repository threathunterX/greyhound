package com.threathunter.greyhound.server.esper.eplgen;

import com.threathunter.common.Utility;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.VariableMeta;
import com.threathunter.util.ClassBasedRegistry;

import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * @author Wen Lu
 */
public class VariableEPLGeneratorRegistry {
    private static final ClassBasedRegistry<VariableMeta, VariableEPLGenerator> registry
            = new ClassBasedRegistry<>(VariableMeta.class);

    static {
        addPackage(VariableEPLGeneratorRegistry.class.getPackage().getName());
        String[] packages = CommonDynamicConfig.getInstance().getStringArray("greyhound.variable.epl.generator.package");
        if (packages != null && packages.length > 0) {
            for (String p : packages) {
                addPackage(p);
            }
        }
    }

    public static void registerVariable(Class<? extends VariableMeta> v, Class<? extends VariableEPLGenerator> g) {
        registry.register(v, g);
    }

    public static Class<? extends VariableEPLGenerator> getGenerator(Class<? extends VariableMeta> v) {
        return registry.get(v);
    }

    /**
     * Add all subclasses in this package to the registry.
     *
     * @param packageName the packages where we can find the subclasses.
     */
    public static void addPackage(String packageName) {
        Set<Class<? extends VariableEPLGenerator>> subclasses = Utility.scannerSubTypeFromPackage(packageName, VariableEPLGenerator.class);
        if (subclasses == null) return;
        for(Class<? extends VariableEPLGenerator> subclass : subclasses) {
            try {
                int modifier = subclass.getModifiers();
                if (Modifier.isInterface(modifier) ||
                        Modifier.isAbstract(modifier)) {
                    continue;
                }
                Type t = subclass.getGenericSuperclass();
                if (!(t instanceof ParameterizedType)) continue;

                ParameterizedType pt = (ParameterizedType)t;
                Type[] arguments = pt.getActualTypeArguments();

                if (arguments == null || arguments.length != 1) {
                    continue;
                }

                Type firstArgument = arguments[0];
                if (firstArgument instanceof Class
                         && VariableMeta.class.isAssignableFrom((Class<?>)firstArgument)) {
                    registry.register((Class<? extends VariableMeta>)firstArgument, subclass);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
